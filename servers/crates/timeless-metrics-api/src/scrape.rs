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
    let client = match reqwest::Client::builder()
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
            Ok(body) => match storage.submit_prometheus(body).await {
                Ok(()) => {
                    controller
                        .update_report(target.id, |report| {
                            report.health = "up".into();
                            report.last_scrape_unix = Some(now);
                            report.last_duration_ms = Some(duration_ms);
                            report.last_error = None;
                            report.samples_scraped = None;
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
            },
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
