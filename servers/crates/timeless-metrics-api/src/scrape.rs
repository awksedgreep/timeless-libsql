use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use reqwest::header::AUTHORIZATION;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use crate::Storage;

const MAX_INTERVAL_SECS: u64 = 86_400;
const MAX_TIMEOUT_SECS: u64 = 300;
const MAX_TARGETS: usize = 10_000;

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct ScrapeTarget {
    pub id: i64,
    pub job_name: String,
    pub scheme: String,
    pub address: String,
    pub metrics_path: String,
    pub scrape_interval_secs: u64,
    pub scrape_timeout_secs: u64,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub auth: Option<ScrapeAuth>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ScrapeAuth {
    #[serde(default)]
    pub bearer: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ScrapeTargetSet {
    pub version: u64,
    pub targets: Vec<ScrapeTarget>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ScrapeTargetReport {
    pub target: ScrapeTarget,
    pub health: String,
    pub last_scrape_unix: Option<u64>,
    pub last_duration_ms: Option<u64>,
    pub last_error: Option<String>,
    pub samples_scraped: Option<u64>,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ScrapeTargetSetReport {
    pub version: u64,
    pub targets: Vec<ScrapeTargetReport>,
}

// -- Response views ---------------------------------------------------------
//
// GET /api/v1/scrape/targets serializes THESE, never the storage types.
// ScrapeAuth carries stored bearer tokens and basic-auth passwords; returning
// it whole let any read-scoped caller (or, with auth disabled, anyone who can
// reach the port) exfiltrate every target's credentials. The views report
// whether credentials are configured without disclosing them. The storage and
// PUT types stay untouched — their Serialize is used for round-trips, and a
// blanket #[serde(skip_serializing)] there would silently drop credentials on
// write.

#[derive(Clone, Debug, Serialize)]
pub struct ScrapeAuthView {
    pub bearer_configured: bool,
    /// Usernames are identifiers, not secrets; passwords never appear.
    pub username: Option<String>,
    pub password_configured: bool,
}

impl From<&ScrapeAuth> for ScrapeAuthView {
    fn from(auth: &ScrapeAuth) -> Self {
        Self {
            bearer_configured: auth.bearer.as_deref().is_some_and(|b| !b.is_empty()),
            username: auth.username.clone(),
            password_configured: auth.password.as_deref().is_some_and(|p| !p.is_empty()),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ScrapeTargetView {
    pub id: i64,
    pub job_name: String,
    pub scheme: String,
    pub address: String,
    pub metrics_path: String,
    pub scrape_interval_secs: u64,
    pub scrape_timeout_secs: u64,
    pub labels: BTreeMap<String, String>,
    pub auth: Option<ScrapeAuthView>,
    pub enabled: bool,
}

impl From<&ScrapeTarget> for ScrapeTargetView {
    fn from(target: &ScrapeTarget) -> Self {
        Self {
            id: target.id,
            job_name: target.job_name.clone(),
            scheme: target.scheme.clone(),
            address: target.address.clone(),
            metrics_path: target.metrics_path.clone(),
            scrape_interval_secs: target.scrape_interval_secs,
            scrape_timeout_secs: target.scrape_timeout_secs,
            labels: target.labels.clone(),
            auth: target.auth.as_ref().map(ScrapeAuthView::from),
            enabled: target.enabled,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ScrapeTargetReportView {
    pub target: ScrapeTargetView,
    pub health: String,
    pub last_scrape_unix: Option<u64>,
    pub last_duration_ms: Option<u64>,
    pub last_error: Option<String>,
    pub samples_scraped: Option<u64>,
}

impl From<&ScrapeTargetReport> for ScrapeTargetReportView {
    fn from(report: &ScrapeTargetReport) -> Self {
        Self {
            target: ScrapeTargetView::from(&report.target),
            health: report.health.clone(),
            last_scrape_unix: report.last_scrape_unix,
            last_duration_ms: report.last_duration_ms,
            last_error: report.last_error.clone(),
            samples_scraped: report.samples_scraped,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ScrapeTargetSetReportView {
    pub version: u64,
    pub targets: Vec<ScrapeTargetReportView>,
}

impl From<&ScrapeTargetSetReport> for ScrapeTargetSetReportView {
    fn from(report: &ScrapeTargetSetReport) -> Self {
        Self {
            version: report.version,
            targets: report
                .targets
                .iter()
                .map(ScrapeTargetReportView::from)
                .collect(),
        }
    }
}

#[derive(Default)]
struct ScrapeState {
    version: u64,
    targets: HashMap<i64, ScrapeTarget>,
    reports: HashMap<i64, ScrapeTargetReport>,
    tasks: HashMap<i64, JoinHandle<()>>,
}

#[derive(Clone, Default)]
pub struct ScrapeController {
    state: std::sync::Arc<Mutex<ScrapeState>>,
}

impl ScrapeController {
    pub async fn replace(&self, storage: Storage, set: ScrapeTargetSet) -> Result<(), String> {
        validate_set(&set)?;
        let mut state = self.state.lock().await;
        if set.version < state.version {
            return Err(format!(
                "scrape target version {} is older than current version {}",
                set.version, state.version
            ));
        }

        if set.version == state.version
            && state.targets.len() == set.targets.len()
            && set
                .targets
                .iter()
                .all(|target| state.targets.get(&target.id) == Some(target))
        {
            return Ok(());
        }

        for task in state.tasks.drain().map(|(_, task)| task) {
            task.abort();
        }
        state.version = set.version;
        state.targets = set
            .targets
            .iter()
            .cloned()
            .map(|target| (target.id, target))
            .collect();
        state.reports.clear();

        for target in set.targets.into_iter() {
            let report = ScrapeTargetReport {
                target: target.clone(),
                health: "unknown".into(),
                ..Default::default()
            };
            state.reports.insert(target.id, report);
            if !target.enabled {
                continue;
            }
            let controller = self.clone();
            let storage = storage.clone();
            let target_id = target.id;
            state.tasks.insert(
                target_id,
                tokio::spawn(async move {
                    scrape_loop(storage, controller, target).await;
                }),
            );
        }
        Ok(())
    }

    pub async fn report(&self) -> ScrapeTargetSetReport {
        let state = self.state.lock().await;
        ScrapeTargetSetReport {
            version: state.version,
            targets: state.reports.values().cloned().collect(),
        }
    }

    pub async fn shutdown(&self) {
        let mut state = self.state.lock().await;
        for task in state.tasks.drain().map(|(_, task)| task) {
            task.abort();
        }
    }

    async fn update_report<F>(&self, target_id: i64, update: F)
    where
        F: FnOnce(&mut ScrapeTargetReport),
    {
        let mut state = self.state.lock().await;
        if let Some(report) = state.reports.get_mut(&target_id) {
            update(report);
        }
    }
}

async fn scrape_loop(storage: Storage, controller: ScrapeController, target: ScrapeTarget) {
    let interval = Duration::from_secs(target.scrape_interval_secs.clamp(1, MAX_INTERVAL_SECS));
    // Resolve and validate before the first request, then pin the connection
    // to the validated address for the life of this loop: a DNS answer that
    // later flips to metadata/link-local space (rebinding) never reaches the
    // socket. A target-set replace rebuilds the loop and re-resolves.
    let pinned = match resolve_validated(&target.address, &target.scheme).await {
        Ok(addrs) => addrs,
        Err(error) => {
            controller
                .update_report(target.id, |report| {
                    report.health = "down".into();
                    report.last_error = Some(error);
                })
                .await;
            return;
        }
    };
    let (host, _) = split_address(&target.address, &target.scheme);
    let client = match reqwest::Client::builder()
        .resolve_to_addrs(&host, &pinned)
        .timeout(Duration::from_secs(
            target.scrape_timeout_secs.clamp(1, MAX_TIMEOUT_SECS),
        ))
        .build()
    {
        Ok(client) => client,
        Err(error) => {
            controller
                .update_report(target.id, |report| {
                    report.health = "down".into();
                    report.last_error = Some(format!("build scrape client: {error}"));
                })
                .await;
            return;
        }
    };

    loop {
        let started = Instant::now();
        let result = scrape_once(&client, &target).await;
        let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(0);

        match result {
            Ok(body) => {
                // Counted here because the body is moved into the ingest queue
                // below and never parsed on this path: submit_prometheus admits
                // it with points: None and the writer parses later, so no count
                // comes back in time to report it.
                let samples = count_samples(&body);

                match storage.submit_prometheus(body).await {
                    Ok(()) => {
                        controller
                            .update_report(target.id, |report| {
                                report.health = "up".into();
                                report.last_scrape_unix = Some(now);
                                report.last_duration_ms = Some(duration_ms);
                                report.last_error = None;
                                report.samples_scraped = Some(samples);
                            })
                            .await;
                    }
                    Err(error) => {
                        controller
                            .update_report(target.id, |report| {
                                report.health = "down".into();
                                report.last_scrape_unix = Some(now);
                                report.last_duration_ms = Some(duration_ms);
                                report.last_error = Some(error);
                            })
                            .await;
                    }
                }
            }
            Err(error) => {
                controller
                    .update_report(target.id, |report| {
                        report.health = "down".into();
                        report.last_scrape_unix = Some(now);
                        report.last_duration_ms = Some(duration_ms);
                        report.last_error = Some(error);
                    })
                    .await;
            }
        }

        tokio::time::sleep(interval).await;
    }
}

async fn scrape_once(client: &reqwest::Client, target: &ScrapeTarget) -> Result<Bytes, String> {
    let scheme = match target.scheme.as_str() {
        "http" | "https" => target.scheme.as_str(),
        other => return Err(format!("unsupported scrape URL scheme {other:?}")),
    };
    let path = if target.metrics_path.starts_with('/') {
        target.metrics_path.clone()
    } else {
        format!("/{}", target.metrics_path)
    };
    let url = format!("{scheme}://{}{}", target.address, path);
    let mut request = client.get(url);
    if let Some(auth) = &target.auth {
        if let Some(token) = &auth.bearer {
            request = request.header(AUTHORIZATION, format!("Bearer {token}"));
        } else if auth.username.is_some() || auth.password.is_some() {
            request = request.basic_auth(
                auth.username.as_deref().unwrap_or_default(),
                auth.password.as_deref(),
            );
        }
    }
    let response = request
        .send()
        .await
        .map_err(|error| format!("scrape request failed: {error}"))?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("scrape returned HTTP {status}"));
    }
    let body = response
        .bytes()
        .await
        .map_err(|error| format!("read scrape response: {error}"))?;
    if body.len() > 10 * 1024 * 1024 {
        return Err("scrape response exceeds 10 MiB".into());
    }
    Ok(decorate_body(body, &target.labels))
}

/// Number of samples in a Prometheus exposition body.
///
/// A byte scan rather than a parse: this runs on every scrape, the body is
/// already in memory, and the exposition format puts exactly one sample on each
/// line that is neither blank nor a comment. Counting what the target exposed is
/// also the honest meaning of "scraped" — how many of those samples storage
/// ultimately keeps is a separate question.
fn count_samples(body: &[u8]) -> u64 {
    body.split(|byte| *byte == b'\n')
        .filter(|line| {
            let line = trim_ascii(line);
            !line.is_empty() && line[0] != b'#'
        })
        .count() as u64
}

fn trim_ascii(mut line: &[u8]) -> &[u8] {
    while let [first, rest @ ..] = line {
        if first.is_ascii_whitespace() {
            line = rest;
        } else {
            break;
        }
    }

    while let [rest @ .., last] = line {
        if last.is_ascii_whitespace() {
            line = rest;
        } else {
            break;
        }
    }

    line
}

fn decorate_body(body: Bytes, labels: &BTreeMap<String, String>) -> Bytes {
    if labels.is_empty() {
        return body;
    }
    let mut output = Vec::with_capacity(body.len() + labels.len() * 24);
    for line in body.split(|byte| *byte == b'\n') {
        if line.is_empty() || line[0] == b'#' {
            output.extend_from_slice(line);
            output.push(b'\n');
            continue;
        }
        let name_end = line
            .iter()
            .position(|byte| *byte == b'{' || byte.is_ascii_whitespace());
        let Some(name_end) = name_end else {
            output.extend_from_slice(line);
            output.push(b'\n');
            continue;
        };
        let mut decorated = Vec::with_capacity(line.len() + labels.len() * 24);
        if line[name_end] == b'{' {
            if let Some(close) = line[name_end + 1..].iter().position(|byte| *byte == b'}') {
                let close = name_end + 1 + close;
                decorated.extend_from_slice(&line[..close]);
                let has_existing = &line[name_end + 1..close];
                for (key, value) in labels {
                    if has_label(has_existing, key) {
                        continue;
                    }
                    if decorated.last() != Some(&b'{') {
                        decorated.push(b',');
                    }
                    append_label(&mut decorated, key, value);
                }
                decorated.extend_from_slice(&line[close..]);
            } else {
                decorated.extend_from_slice(line);
            }
        } else {
            decorated.extend_from_slice(&line[..name_end]);
            decorated.push(b'{');
            append_labels(&mut decorated, labels);
            decorated.push(b'}');
            decorated.extend_from_slice(&line[name_end..]);
        }
        output.extend_from_slice(&decorated);
        output.push(b'\n');
    }
    Bytes::from(output)
}

fn append_labels(output: &mut Vec<u8>, labels: &BTreeMap<String, String>) {
    for (index, (key, value)) in labels.iter().enumerate() {
        if index > 0 {
            output.push(b',');
        }
        append_label(output, key, value);
    }
}

fn append_label(output: &mut Vec<u8>, key: &str, value: &str) {
    output.extend_from_slice(key.as_bytes());
    output.extend_from_slice(b"=\"");
    for byte in value.bytes() {
        match byte {
            b'\\' => output.extend_from_slice(b"\\\\"),
            b'"' => output.extend_from_slice(b"\\\""),
            b'\n' => output.extend_from_slice(b"\\n"),
            other => output.push(other),
        }
    }
    output.push(b'"');
}

fn has_label(labels: &[u8], key: &str) -> bool {
    labels.split(|byte| *byte == b',').any(|part| {
        let part = part.trim_ascii_start();
        let Some(equal) = part.iter().position(|byte| *byte == b'=') else {
            return false;
        };
        let name = part[..equal].trim_ascii();
        name == key.as_bytes()
    })
}

// -- Scrape address safety --------------------------------------------------
//
// A scrape target makes this server issue outbound HTTP(S) requests with
// caller-chosen credentials attached and the response readable back through
// metrics queries — an SSRF primitive if the address space is unrestricted.
// Link-local (which includes cloud instance metadata at 169.254.169.254) and
// unspecified addresses are denied unconditionally, at PUT time for literal
// IPs and again at scrape time for every resolved address. RFC1918/private
// addresses stay allowed: scraping private infrastructure is the normal case.

fn deny_reason(ip: std::net::IpAddr) -> Option<&'static str> {
    use std::net::IpAddr;
    let ip = match ip {
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => IpAddr::V4(v4),
            None => IpAddr::V6(v6),
        },
        v4 => v4,
    };
    match ip {
        IpAddr::V4(v4) if v4.is_link_local() => Some("link-local (instance metadata) address"),
        IpAddr::V4(v4) if v4.is_unspecified() => Some("unspecified address"),
        IpAddr::V4(v4) if v4.is_broadcast() => Some("broadcast address"),
        IpAddr::V6(v6) if (v6.segments()[0] & 0xffc0) == 0xfe80 => {
            Some("link-local (instance metadata) address")
        }
        IpAddr::V6(v6) if v6.is_unspecified() => Some("unspecified address"),
        _ => None,
    }
}

/// Splits a target `address` ("host:port", "[v6]:port", bare host) into
/// (host, port), defaulting the port from the scheme.
fn split_address(address: &str, scheme: &str) -> (String, u16) {
    let default_port = if scheme == "https" { 443 } else { 80 };
    if let Some(rest) = address.strip_prefix('[') {
        if let Some((host, tail)) = rest.split_once(']') {
            let port = tail
                .strip_prefix(':')
                .and_then(|p| p.parse().ok())
                .unwrap_or(default_port);
            return (host.to_owned(), port);
        }
    }
    match address.rsplit_once(':') {
        // A second ':' means an unbracketed IPv6 literal, not host:port.
        Some((host, port)) if !host.contains(':') => match port.parse() {
            Ok(port) => (host.to_owned(), port),
            Err(_) => (address.to_owned(), default_port),
        },
        _ => (address.to_owned(), default_port),
    }
}

fn validate_address(address: &str, scheme: &str) -> Result<(), String> {
    let (host, _port) = split_address(address, scheme);
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        if let Some(reason) = deny_reason(ip) {
            return Err(format!("scrape address {address:?} is a {reason}"));
        }
    }
    Ok(())
}

/// Scrape-time resolution: returns addresses that pass `deny_reason`,
/// erroring if the host resolves only to denied space. The caller pins the
/// connection to a returned address, so a DNS answer that changes after this
/// check (rebinding) cannot redirect the request.
async fn resolve_validated(
    address: &str,
    scheme: &str,
) -> Result<Vec<std::net::SocketAddr>, String> {
    let (host, port) = split_address(address, scheme);
    let resolved: Vec<std::net::SocketAddr> = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|error| format!("resolve scrape address {address:?}: {error}"))?
        .collect();
    if resolved.is_empty() {
        return Err(format!("scrape address {address:?} resolved to nothing"));
    }
    let allowed: Vec<std::net::SocketAddr> = resolved
        .iter()
        .copied()
        .filter(|addr| deny_reason(addr.ip()).is_none())
        .collect();
    if allowed.is_empty() {
        let reason = deny_reason(resolved[0].ip()).unwrap_or("denied address");
        return Err(format!("scrape address {address:?} resolves to a {reason}"));
    }
    Ok(allowed)
}

fn validate_set(set: &ScrapeTargetSet) -> Result<(), String> {
    if set.targets.len() > MAX_TARGETS {
        return Err(format!("scrape target count exceeds {MAX_TARGETS}"));
    }
    let mut ids = std::collections::HashSet::with_capacity(set.targets.len());
    for target in &set.targets {
        if target.id <= 0 || target.job_name.trim().is_empty() || target.address.trim().is_empty() {
            return Err("scrape target id, job_name, and address are required".into());
        }
        if !ids.insert(target.id) {
            return Err(format!("duplicate scrape target id {}", target.id));
        }
        validate_address(&target.address, &target.scheme)?;
        if target.scrape_interval_secs == 0 || target.scrape_interval_secs > MAX_INTERVAL_SECS {
            return Err(format!("scrape target {} has invalid interval", target.id));
        }
        if target.scrape_timeout_secs == 0 || target.scrape_timeout_secs > MAX_TIMEOUT_SECS {
            return Err(format!("scrape target {} has invalid timeout", target.id));
        }
        if target.scrape_timeout_secs > target.scrape_interval_secs {
            return Err(format!(
                "scrape target {} timeout exceeds interval",
                target.id
            ));
        }
        for key in target.labels.keys() {
            if key.is_empty()
                || !key
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            {
                return Err(format!("scrape target {} has invalid label", target.id));
            }
        }
        validate_auth(target.auth.as_ref())?;
    }
    Ok(())
}

fn validate_auth(auth: Option<&ScrapeAuth>) -> Result<(), String> {
    let Some(auth) = auth else {
        return Ok(());
    };
    if auth.bearer.is_some() && (auth.username.is_some() || auth.password.is_some()) {
        return Err("scrape target auth must use bearer or basic credentials".into());
    }
    if auth.username.is_some() ^ auth.password.is_some() {
        return Err("scrape target basic auth requires username and password".into());
    }
    Ok(())
}

fn default_enabled() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_only_sample_lines() {
        // HELP and TYPE lines describe samples; they are not samples.
        let body = b"# HELP up test\n# TYPE up gauge\nup 1\nready 2\n";
        assert_eq!(count_samples(body), 2);
    }

    #[test]
    fn ignores_blank_and_whitespace_lines() {
        // A trailing newline leaves an empty final line, and exporters pad with
        // blank lines between families. Neither is a sample.
        let body = b"up 1\n\n   \nready 2\n";
        assert_eq!(count_samples(body), 2);
    }

    #[test]
    fn tolerates_carriage_returns() {
        // Without trimming, the trailing \r would leave the line non-empty but
        // the count would still be right; the risk is a lone \r line counting as
        // a sample.
        let body = b"up 1\r\n\r\nready 2\r\n";
        assert_eq!(count_samples(body), 2);
    }

    #[test]
    fn counts_indented_comments_as_comments() {
        let body = b"  # HELP up test\nup 1\n";
        assert_eq!(count_samples(body), 1);
    }

    #[test]
    fn an_empty_body_has_no_samples() {
        // The case that matters most: a target answering 200 with nothing must
        // report zero, not be indistinguishable from one that was never counted.
        assert_eq!(count_samples(b""), 0);
        assert_eq!(count_samples(b"\n\n"), 0);
        assert_eq!(count_samples(b"# HELP up test\n"), 0);
    }

    #[test]
    fn decorates_without_overwriting_existing_labels() {
        let body = Bytes::from_static(b"# HELP up test\nup{job=\"x\"} 1\nready 2\n");
        let labels = BTreeMap::from([
            ("job".into(), "new".into()),
            ("cluster".into(), "prod\\east".into()),
        ]);
        let output = decorate_body(body, &labels);
        assert_eq!(
            output,
            Bytes::from_static(
                b"# HELP up test\nup{job=\"x\",cluster=\"prod\\\\east\"} 1\nready{cluster=\"prod\\\\east\",job=\"new\"} 2\n\n"
            )
        );
    }

    #[test]
    fn get_view_never_discloses_credentials() {
        let report = ScrapeTargetSetReport {
            version: 3,
            targets: vec![ScrapeTargetReport {
                target: ScrapeTarget {
                    id: 1,
                    job_name: "demo".into(),
                    scheme: "https".into(),
                    address: "localhost:9090".into(),
                    metrics_path: "/metrics".into(),
                    scrape_interval_secs: 15,
                    scrape_timeout_secs: 10,
                    auth: Some(ScrapeAuth {
                        bearer: Some("SECRET-BEARER-TOKEN".into()),
                        username: Some("scraper".into()),
                        password: Some("SECRET-PASSWORD".into()),
                    }),
                    ..Default::default()
                },
                health: "up".into(),
                ..Default::default()
            }],
        };
        let body = serde_json::to_string(&ScrapeTargetSetReportView::from(&report)).unwrap();
        assert!(
            !body.contains("SECRET-BEARER-TOKEN"),
            "bearer leaked: {body}"
        );
        assert!(!body.contains("SECRET-PASSWORD"), "password leaked: {body}");
        assert!(body.contains("\"bearer_configured\":true"));
        assert!(body.contains("\"password_configured\":true"));
        assert!(body.contains("\"username\":\"scraper\""));
        // The operational signal survives redaction.
        assert!(body.contains("\"address\":\"localhost:9090\""));
    }

    #[test]
    fn denies_metadata_and_link_local_scrape_addresses() {
        for bad in [
            "169.254.169.254",
            "169.254.169.254:80",
            "[fe80::1]:9100",
            "0.0.0.0:9100",
            "[::]:9100",
            "[::ffff:169.254.169.254]:80",
        ] {
            assert!(
                validate_address(bad, "http").is_err(),
                "{bad} must be denied"
            );
        }
        for good in [
            "localhost:9090",
            "127.0.0.1:9090",
            "10.0.0.5:9100",
            "192.168.1.20:9100",
            "prometheus.internal:9090",
            "[2001:db8::1]:9100",
        ] {
            assert!(
                validate_address(good, "http").is_ok(),
                "{good} must be allowed"
            );
        }
    }

    #[tokio::test]
    async fn resolution_rejects_hosts_landing_in_denied_space() {
        // Literal denied IP resolves to itself and must be rejected even
        // though lookup_host succeeds.
        assert!(resolve_validated("169.254.169.254:80", "http")
            .await
            .is_err());
        // Loopback resolves and passes.
        assert!(resolve_validated("127.0.0.1:9090", "http").await.is_ok());
    }

    #[test]
    fn rejects_invalid_target_sets() {
        let target = ScrapeTarget {
            id: 1,
            job_name: "demo".into(),
            address: "localhost:9090".into(),
            scrape_interval_secs: 5,
            scrape_timeout_secs: 6,
            ..Default::default()
        };
        assert!(validate_set(&ScrapeTargetSet {
            version: 1,
            targets: vec![target]
        })
        .is_err());
    }
}
