use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use regex::Regex;

const MATRICES: [&str; 2] = [
    "docs/PROMQL_FEATURE_MATRIX.md",
    "docs/LOGSQL_FEATURE_MATRIX.md",
];
const LEGAL_STATUSES: [&str; 7] = [
    "shipped",
    "partial",
    "in progress",
    "missing",
    "experimental",
    "deferred",
    "library",
];
const LEGAL_TARGETS: [&str; 5] = ["EXT", "API", "SQL", "LIB", "DEFER"];
const LEGAL_PRIORITIES: [&str; 6] = ["P0", "P1", "P2", "P3", "EXP", "DEFER"];
const LEGAL_FINDING_STATUSES: [&str; 4] = ["accepted", "deferred", "experimental", "resolved"];

#[derive(Clone, Debug)]
struct MatrixRow {
    identifier: String,
    status: String,
    target: String,
    priority: String,
    foundation: String,
    source: PathBuf,
    line: usize,
    raw: String,
}

type TestReference = (String, String);
type TestReferences = BTreeMap<String, TestReference>;
type ServerRouteInventory = BTreeMap<(String, String), BTreeSet<String>>;

fn plain(value: &str) -> String {
    value.trim().replace('`', "")
}

fn cells(line: &str) -> Vec<&str> {
    line.trim()
        .trim_matches('|')
        .split('|')
        .map(str::trim)
        .collect()
}

fn unescaped_pipe_count(line: &str) -> usize {
    let mut count = 0;
    let mut backslashes = 0;
    for character in line.chars() {
        if character == '|' && backslashes % 2 == 0 {
            count += 1;
        }
        if character == '\\' {
            backslashes += 1;
        } else {
            backslashes = 0;
        }
    }
    count
}

fn query_storage_finding_ids(root: &Path) -> Result<Vec<(usize, u32)>> {
    let path = root.join("docs/QUERY_STORAGE_FINDINGS.md");
    if !path.is_file() {
        return Ok(Vec::new());
    }
    let identifier = Regex::new(r"^\|\s*`QSF-(\d{3})`\s*\|")?;
    Ok(fs::read_to_string(path)?
        .lines()
        .enumerate()
        .filter_map(|(offset, line)| {
            identifier
                .captures(line)
                .and_then(|captures| captures[1].parse::<u32>().ok())
                .map(|value| (offset + 1, value))
        })
        .collect())
}

fn validate_query_storage_findings(root: &Path) -> Result<Vec<String>> {
    let relative = "docs/QUERY_STORAGE_FINDINGS.md";
    let path = root.join(relative);
    if !path.is_file() {
        return Ok(Vec::new());
    }
    let content = fs::read_to_string(&path)?;
    let identifier = Regex::new(r"^\|\s*`QSF-(\d{3})`\s*\|")?;
    let terminal_status = Regex::new(r"\|\s*([a-z]+)\s*\|\s*$")?;
    let mut errors = Vec::new();
    let mut previous = 0;
    let mut rows = 0;

    for (offset, line) in content.lines().enumerate() {
        let Some(captures) = identifier.captures(line) else {
            continue;
        };
        let number = offset + 1;
        let value = captures[1].parse::<u32>()?;
        rows += 1;
        if value != previous + 1 {
            errors.push(format!(
                "{relative}:{number}: QSF IDs must be contiguous; expected QSF-{:03}, got QSF-{value:03}",
                previous + 1
            ));
        }
        previous = value;

        let expected_pipes = if value <= 7 { 4 } else { 9 };
        let actual_pipes = unescaped_pipe_count(line);
        if actual_pipes != expected_pipes {
            errors.push(format!(
                "{relative}:{number}: QSF-{value:03} has {actual_pipes} unescaped table separators; expected {expected_pipes}"
            ));
        }
        if value > 7 {
            let status = terminal_status
                .captures(line)
                .map(|status| status[1].to_owned());
            if status
                .as_deref()
                .is_none_or(|status| !LEGAL_FINDING_STATUSES.contains(&status))
            {
                errors.push(format!(
                    "{relative}:{number}: QSF-{value:03} has illegal terminal status {status:?}"
                ));
            }
        }
    }
    if rows == 0 {
        errors.push(format!("{relative}: no QSF rows found"));
    }
    Ok(errors)
}

fn parse_matrix(path: &Path) -> Result<(Vec<MatrixRow>, Vec<String>)> {
    let content =
        fs::read_to_string(path).with_context(|| format!("read matrix {}", path.display()))?;
    let mut rows = Vec::new();
    let mut errors = Vec::new();
    let mut header: Option<Vec<String>> = None;

    for (offset, line) in content.lines().enumerate() {
        let number = offset + 1;
        if !line.starts_with('|') {
            header = None;
            continue;
        }
        let values = cells(line);
        let normalized: Vec<String> = values
            .iter()
            .map(|value| plain(value).to_lowercase())
            .collect();
        if normalized.iter().any(|value| value == "id")
            && normalized.iter().any(|value| value == "rust now")
        {
            header = Some(normalized);
            continue;
        }
        let Some(columns) = header.as_ref() else {
            continue;
        };
        if values
            .iter()
            .all(|value| value.chars().all(|ch| ch == '-' || ch == ':'))
        {
            continue;
        }
        let identifier = values.first().map(|value| plain(value)).unwrap_or_default();
        if !identifier.starts_with("PQL-")
            && !identifier.starts_with("MQL-")
            && !identifier.starts_with("LQL-")
        {
            continue;
        }
        if values.len() != columns.len() {
            errors.push(format!(
                "{}:{number}: row has {} cells; expected {}",
                path.display(),
                values.len(),
                columns.len()
            ));
            continue;
        }
        let record: BTreeMap<&str, &str> = columns
            .iter()
            .map(String::as_str)
            .zip(values.iter().copied())
            .collect();
        let required =
            |name: &str| -> Option<String> { record.get(name).map(|value| plain(value)) };
        let (Some(status), Some(target), Some(priority)) = (
            required("rust now"),
            required("target"),
            required("priority"),
        ) else {
            errors.push(format!(
                "{}:{number}: required matrix column missing",
                path.display()
            ));
            continue;
        };
        rows.push(MatrixRow {
            identifier,
            status,
            target,
            priority,
            foundation: required("foundation").unwrap_or_default(),
            source: path.to_path_buf(),
            line: number,
            raw: line.to_owned(),
        });
    }
    Ok((rows, errors))
}

fn heading_anchors(path: &Path) -> Result<BTreeSet<String>> {
    let heading = Regex::new(r"^#{1,6}\s+(.+?)\s*#*\s*$")?;
    let decorations = Regex::new(r"[`*~]")?;
    let illegal = Regex::new(r"[^\p{L}\p{N}_\- ]")?;
    let whitespace = Regex::new(r"\s+")?;
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut anchors = BTreeSet::new();
    for line in fs::read_to_string(path)?.lines() {
        let Some(captures) = heading.captures(line) else {
            continue;
        };
        let value = decorations.replace_all(&captures[1], "").to_lowercase();
        let value = illegal.replace_all(&value, "");
        let anchor = whitespace
            .replace_all(&value, "-")
            .trim_matches('-')
            .to_owned();
        let count = counts.entry(anchor.clone()).or_default();
        let unique = if *count == 0 {
            anchor.clone()
        } else {
            format!("{anchor}-{count}")
        };
        *count += 1;
        anchors.insert(unique);
    }
    Ok(anchors)
}

fn markdown_files(root: &Path) -> Result<Vec<PathBuf>> {
    if root.join(".git").exists() {
        let tracked = Command::new("git")
            .arg("-C")
            .arg(root)
            .args([
                "ls-files",
                "-z",
                "--",
                "README.md",
                "CHANGELOG.md",
                "docs/*.md",
                "servers/crates/*/README.md",
            ])
            .output();
        if let Ok(output) = tracked {
            if output.status.success() {
                let encoded = String::from_utf8(output.stdout)
                    .context("git ls-files returned non-UTF-8 documentation paths")?;
                let mut files: Vec<PathBuf> = encoded
                    .split('\0')
                    .filter(|relative| !relative.is_empty())
                    .map(|relative| root.join(relative))
                    .collect();
                files.sort();
                return Ok(files);
            }
        }
    }

    let mut files = vec![root.join("README.md")];
    let changelog = root.join("CHANGELOG.md");
    if changelog.is_file() {
        files.push(changelog);
    }
    let docs = root.join("docs");
    if docs.is_dir() {
        for entry in fs::read_dir(docs)? {
            let path = entry?.path();
            if path.extension().and_then(|value| value.to_str()) == Some("md") {
                files.push(path);
            }
        }
    }
    let servers = root.join("servers/crates");
    if servers.is_dir() {
        for entry in fs::read_dir(servers)? {
            let path = entry?.path().join("README.md");
            if path.is_file() {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn validate_markdown_table_structure(root: &Path) -> Result<Vec<String>> {
    let mut errors = Vec::new();
    for path in markdown_files(root)? {
        if !path.is_file() {
            continue;
        }
        let relative = path.strip_prefix(root).unwrap_or(&path);
        let mut expected: Option<(usize, usize)> = None;
        let mut fence: Option<char> = None;
        for (offset, line) in fs::read_to_string(&path)?.lines().enumerate() {
            let number = offset + 1;
            let trimmed = line.trim_start();
            let fence_character = if trimmed.starts_with("```") {
                Some('`')
            } else if trimmed.starts_with("~~~") {
                Some('~')
            } else {
                None
            };
            if let Some(character) = fence_character {
                match fence {
                    Some(active) if active == character => fence = None,
                    None => fence = Some(character),
                    Some(_) => {}
                }
                expected = None;
                continue;
            }
            if fence.is_some() {
                continue;
            }
            if !line.starts_with('|') {
                expected = None;
                continue;
            }

            let actual = unescaped_pipe_count(line);
            match expected {
                Some((separators, start)) if actual != separators => errors.push(format!(
                    "{}:{number}: Markdown table started at line {start} has {actual} unescaped separators; expected {separators}",
                    relative.display()
                )),
                None => expected = Some((actual, number)),
                Some(_) => {}
            }
        }
    }
    Ok(errors)
}

fn rust_source_files(directory: &Path, output: &mut Vec<PathBuf>) -> Result<()> {
    if !directory.is_dir() {
        return Ok(());
    }
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            rust_source_files(&path, output)?;
        } else if path.extension().and_then(|value| value.to_str()) == Some("rs") {
            output.push(path);
        }
    }
    Ok(())
}

fn validate_signal_storage_boundaries(root: &Path) -> Result<Vec<String>> {
    let signals = [
        (
            "metrics",
            r#"\b(?:metric_samples|metrics)_(?:chunks|chunks_series_ts|series|meta)\b|format!\s*\(\s*"\{\}_(?:chunks|chunks_series_ts|series|meta)""#,
        ),
        ("logs", r"\blogs_(?:blocks|blocks_ts|terms|meta)\b"),
        (
            "traces",
            r"\btraces_(?:blocks|blocks_ts|terms|trace_blocks|meta)\b",
        ),
    ];
    let mut errors = Vec::new();
    for (signal, pattern) in signals {
        let source_root = root.join(format!("servers/crates/timeless-{signal}-api/src"));
        let mut files = Vec::new();
        rust_source_files(&source_root, &mut files)?;
        files.sort();
        let private_name = Regex::new(pattern)?;
        for path in files {
            let source = fs::read_to_string(&path)?;
            if let Some(found) = private_name.find(&source) {
                let relative = path.strip_prefix(root).unwrap_or(&path).display();
                errors.push(format!(
                    "{relative}: {signal} server references private extension shadow storage `{}`; use a public virtual table, scalar, command, or timeless_stats row",
                    found.as_str()
                ));
            }
            for helper in ["qualified_shadow(", "shadow_object("] {
                if source.contains(helper) {
                    let relative = path.strip_prefix(root).unwrap_or(&path).display();
                    errors.push(format!(
                        "{relative}: {signal} server calls private extension layout helper `{helper}`; use a public virtual table, scalar, command, or timeless_stats row"
                    ));
                }
            }
        }
    }
    Ok(errors)
}

fn validate_public_sql_inventory(root: &Path) -> Result<Vec<String>> {
    let source_root = root.join("crates/timeless-ext/src");
    if !source_root.is_dir() {
        return Ok(Vec::new());
    }
    let module = Regex::new(r#"create_module\s*\(\s*c\"([a-z0-9_]+)\""#)?;
    let scalar = Regex::new(r#"create_scalar_function\s*\(\s*\"([a-z0-9_]+)\""#)?;
    let mut registered = BTreeSet::new();
    for entry in fs::read_dir(&source_root)? {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("rs") {
            continue;
        }
        let source = fs::read_to_string(path)?;
        registered.extend(
            module
                .captures_iter(&source)
                .map(|captures| captures[1].to_owned()),
        );
        registered.extend(
            scalar
                .captures_iter(&source)
                .map(|captures| captures[1].to_owned()),
        );
    }

    let relative = "docs/SQL_API_REFERENCE.md";
    let path = root.join(relative);
    if !path.is_file() {
        return Ok(vec![format!(
            "missing {relative} for {} registered public SQL symbols",
            registered.len()
        )]);
    }
    let content = fs::read_to_string(&path)?;
    let start = "<!-- public-sql-symbols:start -->";
    let stop = "<!-- public-sql-symbols:end -->";
    let Some((_, tail)) = content.split_once(start) else {
        return Ok(vec![format!("{relative}: missing {start} marker")]);
    };
    let Some((inventory, _)) = tail.split_once(stop) else {
        return Ok(vec![format!("{relative}: missing {stop} marker")]);
    };
    let row = Regex::new(r#"(?m)^\|\s*`([a-z0-9_]+)`\s*\|"#)?;
    let documented: BTreeSet<String> = row
        .captures_iter(inventory)
        .map(|captures| captures[1].to_owned())
        .collect();
    let mut errors = Vec::new();
    for name in registered.difference(&documented) {
        errors.push(format!(
            "{relative}: registered public SQL symbol {name} has no inventory row"
        ));
    }
    for name in documented.difference(&registered) {
        errors.push(format!(
            "{relative}: inventory row {name} is not registered by the extension source"
        ));
    }
    Ok(errors)
}

fn marked_region<'a>(
    content: &'a str,
    relative: &str,
    name: &str,
) -> Result<(&'a str, Vec<String>)> {
    let start = format!("<!-- {name}:start -->");
    let stop = format!("<!-- {name}:end -->");
    let Some((_, tail)) = content.split_once(&start) else {
        return Ok(("", vec![format!("{relative}: missing {start} marker")]));
    };
    let Some((region, _)) = tail.split_once(&stop) else {
        return Ok(("", vec![format!("{relative}: missing {stop} marker")]));
    };
    Ok((region, Vec::new()))
}

fn production_source(path: &Path) -> Result<String> {
    let source = fs::read_to_string(path)?;
    Ok(source
        .split_once("#[cfg(test)]")
        .map_or(source.as_str(), |(production, _)| production)
        .to_owned())
}

fn source_routes(source_path: &Path) -> Result<BTreeMap<String, BTreeSet<String>>> {
    let source = production_source(source_path)?;
    let route = Regex::new(r#"(?s)\.route\s*\(\s*\"([^\"]+)\""#)?;
    let method = Regex::new(r"\b(get|post|put|delete|patch|head|options)\s*\(")?;
    let matches: Vec<_> = route.captures_iter(&source).collect();
    let mut routes = BTreeMap::new();
    for (index, captures) in matches.iter().enumerate() {
        let matched = captures.get(0).expect("route match has a complete span");
        let next_route = matches
            .get(index + 1)
            .and_then(|next| next.get(0))
            .map_or(source.len(), |next| next.start());
        let next_fallback = source[matched.end()..]
            .find(".fallback")
            .map_or(source.len(), |offset| matched.end() + offset);
        let end = next_route.min(next_fallback);
        let methods: BTreeSet<String> = method
            .captures_iter(&source[matched.end()..end])
            .map(|captures| captures[1].to_ascii_uppercase())
            .collect();
        let path = captures[1].to_owned();
        if methods.is_empty() {
            bail!(
                "{}: route {path} has no recognized method",
                source_path.display()
            );
        }
        if routes.insert(path.clone(), methods).is_some() {
            bail!("{}: duplicate route {path}", source_path.display());
        }
    }
    Ok(routes)
}

fn documented_server_routes(
    content: &str,
    relative: &str,
) -> Result<(ServerRouteInventory, Vec<String>)> {
    let (region, mut errors) = marked_region(content, relative, "public-server-routes")?;
    let mut routes = BTreeMap::new();
    for (offset, line) in region.lines().enumerate() {
        if !line.starts_with('|') {
            continue;
        }
        let values = cells(line);
        if values.len() < 3 {
            continue;
        }
        let signal = plain(values[0]).to_lowercase();
        if !matches!(signal.as_str(), "metrics" | "logs" | "traces") {
            continue;
        }
        let methods: BTreeSet<String> = plain(values[1])
            .split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_ascii_uppercase)
            .collect();
        let path = plain(values[2]);
        let key = (signal, path);
        if methods.is_empty() {
            errors.push(format!(
                "{relative}:{}: server route has no documented method",
                offset + 1
            ));
        } else if routes.insert(key.clone(), methods).is_some() {
            errors.push(format!(
                "{relative}:{}: duplicate server route {} {}",
                offset + 1,
                key.0,
                key.1
            ));
        }
    }
    Ok((routes, errors))
}

fn validate_public_server_routes(root: &Path) -> Result<Vec<String>> {
    let sources = [
        ("metrics", "servers/crates/timeless-metrics-api/src/api.rs"),
        ("logs", "servers/crates/timeless-logs-api/src/api.rs"),
        ("traces", "servers/crates/timeless-traces-api/src/api.rs"),
    ];
    if !sources
        .iter()
        .any(|(_, relative)| root.join(relative).is_file())
    {
        return Ok(Vec::new());
    }

    let relative = "docs/SERVER_API_REFERENCE.md";
    let path = root.join(relative);
    if !path.is_file() {
        return Ok(vec![format!(
            "missing {relative} for the registered Rust server routes"
        )]);
    }
    let content = fs::read_to_string(path)?;
    let (documented, mut errors) = documented_server_routes(&content, relative)?;
    let mut registered = BTreeMap::new();
    for (signal, source) in sources {
        let path = root.join(source);
        if !path.is_file() {
            continue;
        }
        for (route, methods) in source_routes(&path)? {
            registered.insert((signal.to_owned(), route), methods);
        }
    }

    for ((signal, path), methods) in &registered {
        match documented.get(&(signal.clone(), path.clone())) {
            None => errors.push(format!(
                "{relative}: registered {signal} route {path} has no inventory row"
            )),
            Some(actual) if actual != methods => errors.push(format!(
                "{relative}: {signal} route {path} methods differ; source={methods:?}, documented={actual:?}"
            )),
            Some(_) => {}
        }
    }
    for (signal, path) in documented.keys() {
        if !registered.contains_key(&(signal.clone(), path.clone())) {
            errors.push(format!(
                "{relative}: inventory row {signal} {path} is not registered by a server"
            ));
        }
    }
    Ok(errors)
}

fn source_runtime_environment(root: &Path) -> Result<BTreeSet<String>> {
    let variable = Regex::new(r#"\"(TIMELESS_[A-Z0-9_]+)\""#)?;
    let sources = [
        "servers/crates/timeless-api-common/src/auth.rs",
        "servers/crates/timeless-api-common/src/lib.rs",
        "servers/crates/timeless-metrics-api/src/main.rs",
        "servers/crates/timeless-logs-api/src/main.rs",
        "servers/crates/timeless-traces-api/src/main.rs",
    ];
    let mut variables = BTreeSet::new();
    for relative in sources {
        let path = root.join(relative);
        if !path.is_file() {
            continue;
        }
        let source = production_source(&path)?;
        variables.extend(
            variable
                .captures_iter(&source)
                .map(|captures| captures[1].to_owned())
                .filter(|name| !name.starts_with("TIMELESS_BUILD_")),
        );
    }
    Ok(variables)
}

fn validate_public_server_environment(root: &Path) -> Result<Vec<String>> {
    let registered = source_runtime_environment(root)?;
    if registered.is_empty() {
        return Ok(Vec::new());
    }
    let relative = "docs/SERVER_API_REFERENCE.md";
    let path = root.join(relative);
    if !path.is_file() {
        return Ok(vec![format!(
            "missing {relative} for the Rust server environment"
        )]);
    }
    let content = fs::read_to_string(path)?;
    let (region, mut errors) = marked_region(&content, relative, "public-server-environment")?;
    let row = Regex::new(r#"(?m)^\|\s*`(TIMELESS_[A-Z0-9_]+)`\s*\|"#)?;
    let documented: BTreeSet<String> = row
        .captures_iter(region)
        .map(|captures| captures[1].to_owned())
        .collect();
    for name in registered.difference(&documented) {
        errors.push(format!(
            "{relative}: runtime environment variable {name} has no inventory row"
        ));
    }
    for name in documented.difference(&registered) {
        errors.push(format!(
            "{relative}: environment row {name} is not read by production server source"
        ));
    }
    Ok(errors)
}

fn required_capture(content: &str, pattern: &str, source: &str, field: &str) -> Result<String> {
    let regex = Regex::new(pattern)?;
    regex
        .captures(content)
        .and_then(|captures| captures.get(1))
        .map(|value| value.as_str().to_owned())
        .with_context(|| format!("{source}: cannot derive {field}"))
}

fn pre_one_release_line(version: &str) -> Result<(u64, u64)> {
    let components: Vec<&str> = version.split('.').collect();
    if components.len() != 3 {
        bail!("compatibility version {version:?} is not major.minor.patch");
    }
    let major = components[0]
        .parse::<u64>()
        .with_context(|| format!("invalid major version in {version:?}"))?;
    let minor = components[1]
        .parse::<u64>()
        .with_context(|| format!("invalid minor version in {version:?}"))?;
    components[2]
        .parse::<u64>()
        .with_context(|| format!("invalid patch version in {version:?}"))?;
    Ok((major, minor))
}

fn compatibility_source_versions(root: &Path) -> Result<BTreeMap<String, String>> {
    let root_manifest = root.join("Cargo.toml");
    let server_manifest = root.join("servers/Cargo.toml");
    let extension_source = root.join("crates/timeless-ext/src/capabilities.rs");
    let server_source = root.join("servers/crates/timeless-api-common/src/lib.rs");
    if ![
        root_manifest.as_path(),
        server_manifest.as_path(),
        extension_source.as_path(),
        server_source.as_path(),
    ]
    .iter()
    .all(|path| path.is_file())
    {
        return Ok(BTreeMap::new());
    }

    let root_manifest_text = fs::read_to_string(root_manifest)?;
    let server_manifest_text = fs::read_to_string(server_manifest)?;
    let extension = production_source(&extension_source)?;
    let server = production_source(&server_source)?;
    let mut values = BTreeMap::new();
    values.insert(
        "extension_workspace".to_owned(),
        required_capture(
            &root_manifest_text,
            r#"(?m)^version\s*=\s*\"([^\"]+)\"\s*$"#,
            "Cargo.toml",
            "workspace version",
        )?,
    );
    values.insert(
        "server_workspace".to_owned(),
        required_capture(
            &server_manifest_text,
            r#"(?m)^version\s*=\s*\"([^\"]+)\"\s*$"#,
            "servers/Cargo.toml",
            "workspace version",
        )?,
    );
    values.insert(
        "extension_data_abi".to_owned(),
        required_capture(
            &extension,
            r"(?m)^const DATA_ABI:\s*u64\s*=\s*(\d+);$",
            "crates/timeless-ext/src/capabilities.rs",
            "data ABI",
        )?,
    );
    values.insert(
        "sql_surface_version".to_owned(),
        required_capture(
            &extension,
            r#"\"sql_surface_version\"\s*:\s*(\d+)"#,
            "crates/timeless-ext/src/capabilities.rs",
            "SQL surface version",
        )?,
    );
    values.insert(
        "extension_minimum_server".to_owned(),
        required_capture(
            &extension,
            r#"\"minimum_server_version\"\s*:\s*\"([^\"]+)\""#,
            "crates/timeless-ext/src/capabilities.rs",
            "minimum server version",
        )?,
    );
    values.insert(
        "server_data_schema".to_owned(),
        required_capture(
            &server,
            r"(?m)^pub const DATA_SCHEMA_VERSION:\s*i64\s*=\s*(\d+);$",
            "servers/crates/timeless-api-common/src/lib.rs",
            "server data schema",
        )?,
    );
    values.insert(
        "server_required_data_abi".to_owned(),
        required_capture(
            &server,
            r"(?m)^pub const REQUIRED_EXTENSION_DATA_ABI:\s*u64\s*=\s*(\d+);$",
            "servers/crates/timeless-api-common/src/lib.rs",
            "required extension data ABI",
        )?,
    );
    values.insert(
        "server_minimum_extension".to_owned(),
        required_capture(
            &server,
            r#"(?m)^pub const MINIMUM_EXTENSION_VERSION:\s*&str\s*=\s*\"([^\"]+)\";$"#,
            "servers/crates/timeless-api-common/src/lib.rs",
            "minimum extension version",
        )?,
    );
    Ok(values)
}

fn validate_public_compatibility_versions(root: &Path) -> Result<Vec<String>> {
    let expected = compatibility_source_versions(root)?;
    if expected.is_empty() {
        return Ok(Vec::new());
    }

    let relative = "docs/COMPATIBILITY.md";
    let path = root.join(relative);
    if !path.is_file() {
        return Ok(vec![format!(
            "missing {relative} for the public compatibility generations"
        )]);
    }
    let content = fs::read_to_string(path)?;
    let (region, mut errors) = marked_region(&content, relative, "public-compatibility-versions")?;
    let row = Regex::new(r#"(?m)^\|\s*`([^`]+)`\s*\|\s*`([^`]+)`\s*\|"#)?;
    let documented: BTreeMap<String, String> = row
        .captures_iter(region)
        .map(|captures| (captures[1].to_owned(), captures[2].to_owned()))
        .collect();
    for (key, value) in &expected {
        match documented.get(key) {
            None => errors.push(format!(
                "{relative}: compatibility key {key} has no inventory row"
            )),
            Some(actual) if actual != value => errors.push(format!(
                "{relative}: compatibility key {key} differs; source={value:?}, documented={actual:?}"
            )),
            Some(_) => {}
        }
    }
    for key in documented.keys() {
        if !expected.contains_key(key) {
            errors.push(format!(
                "{relative}: compatibility inventory key {key} has no source contract"
            ));
        }
    }

    if expected.get("extension_workspace") != expected.get("server_workspace") {
        errors.push(format!(
            "Cargo.toml and servers/Cargo.toml release versions differ: extension={:?}, server={:?}",
            expected.get("extension_workspace"),
            expected.get("server_workspace")
        ));
    }
    for (workspace, floor, description) in [
        (
            "extension_workspace",
            "extension_minimum_server",
            "extension workspace and minimum server",
        ),
        (
            "server_workspace",
            "server_minimum_extension",
            "server workspace and minimum extension",
        ),
    ] {
        let workspace_line = pre_one_release_line(
            expected
                .get(workspace)
                .expect("compatibility source inventory has workspace"),
        )?;
        let floor_line = pre_one_release_line(
            expected
                .get(floor)
                .expect("compatibility source inventory has floor"),
        )?;
        if workspace_line != floor_line {
            errors.push(format!(
                "{description} versions must use the same pre-1.0 compatibility line"
            ));
        }
    }
    if expected.get("extension_data_abi") != expected.get("server_required_data_abi") {
        errors.push("extension and server data ABI contracts differ".to_owned());
    }

    let changelog_relative = "CHANGELOG.md";
    let changelog = root.join(changelog_relative);
    if !changelog.is_file() {
        errors.push(format!("missing {changelog_relative}"));
    } else {
        let changelog_content = fs::read_to_string(changelog)?;
        let target = required_capture(
            &changelog_content,
            r"<!--\s*release-target:\s*([^\s]+)\s*-->",
            changelog_relative,
            "release target",
        )?;
        if Some(&target) != expected.get("extension_workspace") {
            errors.push(format!(
                "{changelog_relative}: release target {target:?} differs from workspace {:?}",
                expected.get("extension_workspace")
            ));
        }
    }
    Ok(errors)
}

fn inline_code(value: &str) -> Result<Vec<String>> {
    let code = Regex::new(r"`([^`]+)`")?;
    Ok(code
        .captures_iter(value)
        .map(|captures| captures[1].to_owned())
        .collect())
}

fn validate_public_artifact_inventory(root: &Path) -> Result<Vec<String>> {
    let source_relative = "tools/release-tool/artifact-inventory.json";
    let source_path = root.join(source_relative);
    if !source_path.is_file() {
        return Ok(Vec::new());
    }
    let inventory: serde_json::Value = serde_json::from_str(&fs::read_to_string(&source_path)?)
        .with_context(|| format!("decode {source_relative}"))?;
    let document_relative = "docs/ARTIFACTS.md";
    let document_path = root.join(document_relative);
    if !document_path.is_file() {
        return Ok(vec![format!(
            "missing {document_relative} for the native package inventory"
        )]);
    }
    let document = fs::read_to_string(&document_path)?;
    let mut errors = Vec::new();

    let expected_targets: BTreeMap<String, String> = inventory
        .get("targets")
        .and_then(serde_json::Value::as_array)
        .context("artifact inventory targets must be an array")?
        .iter()
        .map(|target| {
            let triple = target
                .get("triple")
                .and_then(serde_json::Value::as_str)
                .context("artifact inventory target is missing triple")?;
            let suffix = target
                .get("extension_suffix")
                .and_then(serde_json::Value::as_str)
                .context("artifact inventory target is missing extension_suffix")?;
            Ok((triple.to_owned(), format!("lib/libtimeless_ext.{suffix}")))
        })
        .collect::<Result<_>>()?;
    if expected_targets.is_empty() {
        bail!("{source_relative}: targets has no entries");
    }

    let (target_region, mut marker_errors) =
        marked_region(&document, document_relative, "public-artifact-targets")?;
    errors.append(&mut marker_errors);
    let mut documented_targets = BTreeMap::new();
    for (offset, line) in target_region.lines().enumerate() {
        if !line.starts_with('|') {
            continue;
        }
        let values = cells(line);
        if values.len() != 3 {
            continue;
        }
        let target = inline_code(values[0])?;
        let extension = inline_code(values[2])?;
        if target.len() != 1 || !target[0].contains("-") {
            continue;
        }
        if extension.len() != 1 {
            errors.push(format!(
                "{document_relative}:{}: artifact target must name exactly one extension path",
                offset + 1
            ));
            continue;
        }
        if documented_targets
            .insert(target[0].clone(), extension[0].clone())
            .is_some()
        {
            errors.push(format!(
                "{document_relative}:{}: duplicate artifact target {}",
                offset + 1,
                target[0]
            ));
        }
    }
    if documented_targets != expected_targets {
        errors.push(format!(
            "{document_relative}: native target inventory differs; source={expected_targets:?}, documented={documented_targets:?}"
        ));
    }

    let mut expected_files: BTreeSet<String> = inventory
        .get("binaries")
        .and_then(serde_json::Value::as_array)
        .context("artifact inventory binaries must be an array")?
        .iter()
        .map(|binary| {
            binary
                .as_str()
                .map(|binary| format!("bin/{binary}"))
                .context("artifact inventory binary must be a string")
        })
        .collect::<Result<_>>()?;
    expected_files.extend(
        inventory
            .get("fixed_files")
            .and_then(serde_json::Value::as_array)
            .context("artifact inventory fixed_files must be an array")?
            .iter()
            .map(|path| {
                path.as_str()
                    .map(str::to_owned)
                    .context("artifact inventory fixed file must be a string")
            })
            .collect::<Result<Vec<_>>>()?,
    );
    for suffix in expected_targets
        .values()
        .filter_map(|path| path.rsplit_once('.'))
    {
        expected_files.insert(format!("{}.{}", suffix.0, suffix.1));
    }

    let (file_region, mut marker_errors) =
        marked_region(&document, document_relative, "public-artifact-files")?;
    errors.append(&mut marker_errors);
    let mut documented_files = BTreeSet::new();
    for line in file_region.lines() {
        if !line.starts_with('|') {
            continue;
        }
        let values = cells(line);
        if values.len() != 2 {
            continue;
        }
        for path in inline_code(values[0])? {
            if path.contains('/') || path.contains('.') || expected_files.contains(&path) {
                documented_files.insert(path);
            }
        }
    }
    if documented_files != expected_files {
        errors.push(format!(
            "{document_relative}: archive file inventory differs; source={expected_files:?}, documented={documented_files:?}"
        ));
    }
    Ok(errors)
}

fn validate_public_embedding_contract(root: &Path) -> Result<Vec<String>> {
    let manifest_relative = "crates/timeless-ext/Cargo.toml";
    let manifest_path = root.join(manifest_relative);
    if !manifest_path.is_file() {
        return Ok(Vec::new());
    }
    let manifest = fs::read_to_string(&manifest_path)?;
    let document_relative = "docs/EMBEDDED_RUST.md";
    let document_path = root.join(document_relative);
    if !document_path.is_file() {
        return Ok(vec![format!(
            "missing {document_relative} for the public Rust embedding API"
        )]);
    }
    let document = fs::read_to_string(&document_path)?;
    let rusqlite_version = required_capture(
        &manifest,
        r#"(?m)^rusqlite\s*=\s*\{\s*version\s*=\s*"([^"]+)""#,
        manifest_relative,
        "rusqlite version",
    )?;
    let lock_relative = "tools/libsql-check/Cargo.lock";
    let lock = fs::read_to_string(root.join(lock_relative))?;
    let libsql_version = required_capture(
        &lock,
        r#"(?ms)^name\s*=\s*"libsql"\s*\nversion\s*=\s*"([^"]+)""#,
        lock_relative,
        "libsql gate version",
    )?;
    let expected = BTreeMap::from([
        ("direct_libsql_gate_version".to_owned(), libsql_version),
        (
            "dynamic_libsql_gate".to_owned(),
            "tools/libsql-check/src/main.rs".to_owned(),
        ),
        ("rusqlite_version".to_owned(), rusqlite_version),
        (
            "static_example".to_owned(),
            "crates/timeless-ext/examples/embedded.rs".to_owned(),
        ),
        (
            "timeless_ext_embedded_feature".to_owned(),
            "embedded".to_owned(),
        ),
        (
            "timeless_ext_loadable_feature".to_owned(),
            "entrypoints".to_owned(),
        ),
    ]);
    let (region, mut errors) =
        marked_region(&document, document_relative, "public-embedding-contract")?;
    let row = Regex::new(r#"(?m)^\|\s*`([^`]+)`\s*\|\s*`([^`]+)`\s*\|"#)?;
    let documented: BTreeMap<String, String> = row
        .captures_iter(region)
        .map(|captures| (captures[1].to_owned(), captures[2].to_owned()))
        .collect();
    if documented != expected {
        errors.push(format!(
            "{document_relative}: embedding contract differs; source={expected:?}, documented={documented:?}"
        ));
    }

    for (needle, description) in [
        (
            "entrypoints = [\"rusqlite/loadable_extension\"]",
            "entrypoints must select rusqlite loadable-extension mode",
        ),
        ("embedded = []", "embedded feature must remain explicit"),
        (
            "required-features = [\"embedded\"]",
            "embedded example must require the linked feature",
        ),
    ] {
        if !manifest.contains(needle) {
            errors.push(format!("{manifest_relative}: {description}"));
        }
    }

    let library_relative = "crates/timeless-ext/src/lib.rs";
    let library = fs::read_to_string(root.join(library_relative))?;
    for needle in [
        "pub fn register_telemetry",
        "pub fn register_dbhealth",
        "feature = \"entrypoints\"",
        "feature = \"embedded\"",
    ] {
        if !library.contains(needle) {
            errors.push(format!(
                "{library_relative}: embedding source is missing {needle}"
            ));
        }
    }

    for relative in [
        "crates/timeless-ext/examples/embedded.rs",
        "tools/libsql-check/src/main.rs",
    ] {
        let path = root.join(relative);
        if !path.is_file() {
            errors.push(format!("missing {relative}"));
            continue;
        }
        let source = fs::read_to_string(path)?;
        for needle in [
            "timeless_metrics",
            "timeless_logs",
            "timeless_traces",
            "status_description",
            "events",
            "resource",
            "instrumentation_scope",
        ] {
            if !source.contains(needle) {
                errors.push(format!("{relative}: production smoke is missing {needle}"));
            }
        }
        if source.contains("USING timeless_spike") {
            errors.push(format!(
                "{relative}: production embedding smoke must not create timeless_spike"
            ));
        }
    }
    let gate_relative = "tools/libsql-check/src/main.rs";
    let gate = fs::read_to_string(root.join(gate_relative))?;
    if gate.matches("Builder::new_local(&database_path)").count() < 2 {
        errors.push(format!(
            "{gate_relative}: direct libSQL gate must close and reopen the durable database"
        ));
    }
    Ok(errors)
}

fn validate_canonical_documentation_wording(root: &Path) -> Result<Vec<String>> {
    let forbidden = [
        (":latest", "floating container tag"),
        ("built from libsql main", "floating libSQL branch"),
        ("shadow-table inspection", "private shadow-table inspection"),
        ("everything works verbatim", "unbounded compatibility claim"),
    ];
    let mut errors = Vec::new();
    for relative in [
        "README.md",
        "docs/GUIDE.md",
        "docs/SQL_API_REFERENCE.md",
        "docs/SERVER_API_REFERENCE.md",
        "docs/EMBEDDED_RUST.md",
        "docs/SQLD.md",
        "docs/ARTIFACTS.md",
    ] {
        let path = root.join(relative);
        if !path.is_file() {
            continue;
        }
        let content = fs::read_to_string(path)?.to_lowercase();
        for (needle, description) in forbidden {
            if content.contains(needle) {
                errors.push(format!("{relative}: contains stale {description}"));
            }
        }
    }
    for (relative, marker) in [
        ("PLAN.md", "<!-- document-status: historical-design -->"),
        (
            "RESULTS.md",
            "<!-- document-status: historical-benchmark -->",
        ),
    ] {
        let path = root.join(relative);
        if path.is_file() && !fs::read_to_string(path)?.contains(marker) {
            errors.push(format!(
                "{relative}: development history must carry {marker}"
            ));
        }
    }
    Ok(errors)
}

fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let hex = |byte: u8| match byte {
                b'0'..=b'9' => Some(byte - b'0'),
                b'a'..=b'f' => Some(byte - b'a' + 10),
                b'A'..=b'F' => Some(byte - b'A' + 10),
                _ => None,
            };
            if let (Some(high), Some(low)) = (hex(bytes[index + 1]), hex(bytes[index + 2])) {
                decoded.push((high << 4) | low);
                index += 3;
                continue;
            }
        }
        decoded.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}

fn validate_local_links(root: &Path) -> Result<Vec<String>> {
    let links = Regex::new(r"\[[^\]]*\]\(([^)]+)\)")?;
    let mut errors = Vec::new();
    for source in markdown_files(root)? {
        if !source.exists() {
            continue;
        }
        let content = fs::read_to_string(&source)?;
        for captures in links.captures_iter(&content) {
            let raw = &captures[1];
            if raw.starts_with("http://")
                || raw.starts_with("https://")
                || raw.starts_with("mailto:")
                || raw.starts_with('#')
            {
                continue;
            }
            let decoded = percent_decode(raw);
            let (location, anchor) = decoded
                .split_once('#')
                .map_or((decoded.as_str(), None), |(path, anchor)| {
                    (path, Some(anchor))
                });
            let target = if location.is_empty() {
                source.clone()
            } else {
                source.parent().unwrap_or(root).join(location)
            };
            if !target.exists() {
                errors.push(format!(
                    "{}: broken local link {raw}",
                    source.strip_prefix(root).unwrap_or(&source).display()
                ));
                continue;
            }
            if let Some(anchor) = anchor {
                if target.extension().and_then(|value| value.to_str()) == Some("md")
                    && !heading_anchors(&target)?.contains(anchor)
                {
                    errors.push(format!(
                        "{}: missing anchor {raw}",
                        source.strip_prefix(root).unwrap_or(&source).display()
                    ));
                }
            }
        }
    }
    Ok(errors)
}

fn parse_test_references(path: &Path) -> Result<(TestReferences, Vec<String>)> {
    let content = fs::read_to_string(path)?;
    let mut references = BTreeMap::new();
    let mut errors = Vec::new();
    let mut header: Option<Vec<String>> = None;
    for (offset, line) in content.lines().enumerate() {
        let number = offset + 1;
        if !line.starts_with('|') {
            header = None;
            continue;
        }
        let values = cells(line);
        let normalized: Vec<String> = values
            .iter()
            .map(|value| plain(value).to_lowercase())
            .collect();
        if normalized.get(0..3) == Some(&["id".into(), "test path".into(), "test symbol".into()]) {
            header = Some(normalized);
            continue;
        }
        let Some(columns) = header.as_ref() else {
            continue;
        };
        if values
            .iter()
            .all(|value| value.chars().all(|ch| ch == '-' || ch == ':'))
        {
            continue;
        }
        let identifier = values.first().map(|value| plain(value)).unwrap_or_default();
        if !identifier.starts_with("PQL-")
            && !identifier.starts_with("MQL-")
            && !identifier.starts_with("LQL-")
        {
            continue;
        }
        if references.contains_key(&identifier) {
            errors.push(format!(
                "{}:{number}: duplicate test reference {identifier}",
                path.display()
            ));
            continue;
        }
        let record: BTreeMap<&str, &str> = columns
            .iter()
            .map(String::as_str)
            .zip(values.iter().copied())
            .collect();
        let test_path = record
            .get("test path")
            .map(|value| plain(value))
            .unwrap_or_default();
        let symbol = record
            .get("test symbol")
            .map(|value| plain(value))
            .unwrap_or_default();
        references.insert(identifier, (test_path, symbol));
    }
    Ok((references, errors))
}

fn shipped_marker(path: &Path) -> Result<(BTreeSet<String>, Vec<String>)> {
    let marker = Regex::new(r"<!--\s*query-contract-shipped:\s*(.*?)\s*-->")?;
    let content = fs::read_to_string(path)?;
    let matches: Vec<_> = marker.captures_iter(&content).collect();
    if matches.len() != 1 {
        return Ok((
            BTreeSet::new(),
            vec![format!(
                "{}: expected exactly one query-contract-shipped marker",
                path.display()
            )],
        ));
    }
    Ok((
        matches[0][1]
            .split_whitespace()
            .map(str::to_owned)
            .collect(),
        Vec::new(),
    ))
}

fn matrix_terminal_summary(rows: &[MatrixRow]) -> String {
    let mut groups: BTreeMap<&str, BTreeMap<&str, usize>> = BTreeMap::new();
    for row in rows {
        let family = row.identifier.split('-').next().unwrap_or("unknown");
        *groups
            .entry(family)
            .or_default()
            .entry(row.status.as_str())
            .or_default() += 1;
    }
    groups
        .into_iter()
        .map(|(family, statuses)| {
            let statuses = statuses
                .into_iter()
                .map(|(status, count)| format!("{status}={count}"))
                .collect::<Vec<_>>()
                .join(",");
            format!("{family} {statuses}")
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn latency_triplet(evidence: &serde_json::Value, signal: &str, query: &str) -> Option<String> {
    let latency = evidence.pointer(&format!("/{signal}/queries/{query}/latency_ns"))?;
    let millis = |name: &str| {
        latency
            .get(name)?
            .as_u64()
            .map(|value| value as f64 / 1_000_000.0)
    };
    Some(format!(
        "{:.3} / {:.3} / {:.3}",
        millis("p50")?,
        millis("p95")?,
        millis("p99")?
    ))
}

fn validate_query_release_fault_evidence(
    root: &Path,
    report: &str,
    report_relative: &str,
) -> Result<Vec<String>> {
    let mut errors = Vec::new();
    let evidence_marker = Regex::new(r"<!--\s*query-release-fault-evidence:\s*([^\s]+)\s*-->")?;
    let Some(evidence_relative) = evidence_marker
        .captures(report)
        .map(|captures| captures[1].to_owned())
    else {
        return Ok(vec![format!(
            "{report_relative}: missing query-release-fault-evidence marker"
        )]);
    };
    if !evidence_relative.starts_with("docs/evidence/") || evidence_relative.contains("..") {
        return Ok(vec![format!(
            "{report_relative}: fault evidence must be an owned docs/evidence path"
        )]);
    }
    let evidence_path = root.join(&evidence_relative);
    if !evidence_path.is_file() {
        return Ok(vec![format!(
            "{report_relative}: fault evidence does not exist: {evidence_relative}"
        )]);
    }
    let evidence: serde_json::Value = serde_json::from_str(&fs::read_to_string(&evidence_path)?)
        .with_context(|| format!("decode fault evidence {evidence_relative}"))?;

    if evidence.get("verdict").and_then(serde_json::Value::as_str) != Some("passed") {
        errors.push(format!("{evidence_relative}: fault verdict is not passed"));
    }
    if !matches!(
        evidence.get("mode").and_then(serde_json::Value::as_str),
        Some("short" | "release")
    ) {
        errors.push(format!("{evidence_relative}: unknown fault-gate mode"));
    }
    if evidence
        .get("configured_duration_seconds")
        .and_then(serde_json::Value::as_f64)
        .is_none_or(|duration| duration < 120.0)
    {
        errors.push(format!(
            "{evidence_relative}: fault-gate duration is shorter than 120 seconds"
        ));
    }
    if evidence
        .get("failures")
        .and_then(serde_json::Value::as_array)
        .is_none_or(|failures| !failures.is_empty())
    {
        errors.push(format!(
            "{evidence_relative}: fault-gate failures are present"
        ));
    }

    let faults = evidence.get("faults").and_then(serde_json::Value::as_array);
    let fault_names = faults
        .into_iter()
        .flatten()
        .filter_map(|fault| fault.get("fault").and_then(serde_json::Value::as_str))
        .collect::<Vec<_>>();
    for required in [
        "metrics_startup_descriptor_disk_faults",
        "logs_startup_descriptor_disk_faults",
        "traces_startup_descriptor_disk_faults",
        "slow_disconnect_cancellation_storm",
        "metrics_backup_overlap",
        "logs_backup_overlap",
        "traces_backup_overlap",
        "graceful_restart",
        "sigkill_restart",
    ] {
        if !fault_names.contains(&required) {
            errors.push(format!(
                "{evidence_relative}: required fault {required} was not exercised"
            ));
        }
    }
    if faults.is_none_or(|events| {
        events
            .iter()
            .any(|event| event.get("result").and_then(serde_json::Value::as_str) != Some("passed"))
    }) {
        errors.push(format!(
            "{evidence_relative}: one or more fault events did not pass"
        ));
    }

    let mut completed = Vec::new();
    for signal in ["metrics", "logs", "traces"] {
        let signal_pointer = format!("/signals/{signal}");
        let signal_evidence = evidence.pointer(&signal_pointer);
        let durable = signal_evidence
            .and_then(|value| value.get("accepted_and_durable_records"))
            .and_then(serde_json::Value::as_u64);
        if durable.is_none_or(|value| value == 0) {
            errors.push(format!(
                "{evidence_relative}: {signal} has no accepted durable work"
            ));
        } else if let Some(durable) = durable {
            completed.push(durable);
        }
        if signal_evidence
            .and_then(|value| value.get("errors"))
            .and_then(serde_json::Value::as_array)
            .is_none_or(|signal_errors| !signal_errors.is_empty())
        {
            errors.push(format!("{evidence_relative}: {signal} errors are present"));
        }
        if signal_evidence
            .and_then(|value| value.get("process_generations"))
            .and_then(serde_json::Value::as_u64)
            .is_none_or(|generations| generations < 5)
        {
            errors.push(format!(
                "{evidence_relative}: {signal} did not survive the scheduled restarts"
            ));
        }
        let rss = signal_evidence
            .and_then(|value| value.get("rss_hwm_kib"))
            .and_then(serde_json::Value::as_u64);
        let rss_limit = evidence
            .pointer(&format!("/limits/max_rss_kib/{signal}"))
            .and_then(serde_json::Value::as_u64);
        if rss.is_none() || rss_limit.is_none() || rss > rss_limit {
            errors.push(format!(
                "{evidence_relative}: {signal} RSS HWM is absent or above its declared limit"
            ));
        }
        if evidence
            .pointer(&format!("/final_barriers/{signal}/status"))
            .and_then(serde_json::Value::as_str)
            != Some("ok")
        {
            errors.push(format!(
                "{evidence_relative}: {signal} final durability barrier is not ok"
            ));
        }
    }
    completed.sort_unstable();
    completed.dedup();
    if completed.len() != 1 {
        errors.push(format!(
            "{evidence_relative}: signals do not report equal durable work"
        ));
    }

    let event_count = faults.map_or(0, Vec::len);
    let records_per_signal = completed.first().copied().unwrap_or_default();
    let expected_summary = format!("events={event_count} records_per_signal={records_per_signal}");
    let summary_marker = Regex::new(r"<!--\s*query-release-fault-summary:\s*(.*?)\s*-->")?;
    let actual_summary = summary_marker
        .captures(report)
        .map(|captures| captures[1].trim().to_owned());
    if actual_summary.as_deref() != Some(expected_summary.as_str()) {
        errors.push(format!(
            "{report_relative}: fault summary differs; expected {expected_summary:?}, got {actual_summary:?}"
        ));
    }

    Ok(errors)
}

fn validate_query_release_report(root: &Path, rows: &[MatrixRow]) -> Result<Vec<String>> {
    let plan = root.join("docs/2026-08-04_query_surface_implementation_plan.md");
    if !plan.is_file() {
        return Ok(Vec::new());
    }

    let relative = "docs/QUERY_RELEASE_REPORT.md";
    let path = root.join(relative);
    if !path.is_file() {
        return Ok(vec![format!("missing {relative}")]);
    }
    let content = fs::read_to_string(&path)?;
    let mut errors = Vec::new();

    match query_storage_finding_ids(root)?.last().copied() {
        Some((_, last)) => {
            let expected = format!("through `QSF-{last:03}`");
            if !content.contains(&expected) {
                errors.push(format!(
                    "{relative}: storage-finding range is stale; expected {expected:?}"
                ));
            }
        }
        None => errors.push(format!(
            "{relative}: cannot derive the storage-finding range"
        )),
    }

    let summary_marker = Regex::new(r"<!--\s*query-release-matrix-summary:\s*(.*?)\s*-->")?;
    let expected_summary = matrix_terminal_summary(rows);
    let actual_summary = summary_marker
        .captures(&content)
        .map(|captures| captures[1].trim().to_owned());
    if actual_summary.as_deref() != Some(expected_summary.as_str()) {
        errors.push(format!(
            "{relative}: matrix summary differs; expected {expected_summary:?}, got {actual_summary:?}"
        ));
    }

    for row in rows.iter().filter(|row| row.status != "shipped") {
        if !content.contains(&format!("`{}`", row.identifier)) {
            errors.push(format!(
                "{relative}: terminal non-shipped row {} has no explicit report disposition",
                row.identifier
            ));
        }
    }

    errors.extend(validate_query_release_fault_evidence(
        root, &content, relative,
    )?);

    let evidence_marker = Regex::new(r"<!--\s*query-release-evidence:\s*([^\s]+)\s*-->")?;
    let Some(evidence_relative) = evidence_marker
        .captures(&content)
        .map(|captures| captures[1].to_owned())
    else {
        errors.push(format!("{relative}: missing query-release-evidence marker"));
        return Ok(errors);
    };
    if !evidence_relative.starts_with("docs/evidence/") || evidence_relative.contains("..") {
        errors.push(format!(
            "{relative}: release evidence must be an owned docs/evidence path"
        ));
        return Ok(errors);
    }
    let evidence_path = root.join(&evidence_relative);
    if !evidence_path.is_file() {
        errors.push(format!(
            "{relative}: release evidence does not exist: {evidence_relative}"
        ));
        return Ok(errors);
    }
    let evidence: serde_json::Value = serde_json::from_str(&fs::read_to_string(&evidence_path)?)
        .with_context(|| format!("decode release evidence {evidence_relative}"))?;
    let source_commit = evidence
        .get("git_commit")
        .and_then(serde_json::Value::as_str);
    for pointer in [
        "/extension_build/commit",
        "/metrics/build/commit",
        "/logs/build/commit",
    ] {
        if evidence
            .pointer(pointer)
            .and_then(serde_json::Value::as_str)
            != source_commit
        {
            errors.push(format!(
                "{evidence_relative}: {pointer} does not match the evidence source commit"
            ));
        }
    }
    if source_commit.is_none_or(|commit| !content.contains(commit)) {
        errors.push(format!(
            "{relative}: report does not name the exact measured source commit"
        ));
    }

    for (signal, completed_pointer, fixture_pointer) in [
        (
            "metrics",
            "/metrics/ingestion/completed_points",
            "/metrics/fixture/logical_points",
        ),
        (
            "logs",
            "/logs/ingestion/completed_entries",
            "/logs/fixture/logical_entries",
        ),
    ] {
        let completed = evidence
            .pointer(completed_pointer)
            .and_then(serde_json::Value::as_u64);
        let fixture = evidence
            .pointer(fixture_pointer)
            .and_then(serde_json::Value::as_u64);
        if completed.is_none() || completed != fixture {
            errors.push(format!(
                "{evidence_relative}: {signal} durable completed work does not equal the fixture"
            ));
        }
    }
    for pointer in [
        "/metrics/ingestion/failed_points",
        "/metrics/ingestion/queued_points",
        "/logs/ingestion/queued_entries",
        "/logs/cancellation/in_flight_at_capture",
    ] {
        if evidence
            .pointer(pointer)
            .and_then(serde_json::Value::as_u64)
            != Some(0)
        {
            errors.push(format!(
                "{evidence_relative}: {pointer} must be zero at the release evidence barrier"
            ));
        }
    }

    let metric_shapes = evidence
        .pointer("/metrics/queries")
        .and_then(serde_json::Value::as_object)
        .map(serde_json::Map::len)
        .unwrap_or_default();
    let log_shapes = evidence
        .pointer("/logs/queries")
        .and_then(serde_json::Value::as_object)
        .map(serde_json::Map::len)
        .unwrap_or_default();
    let iterations = evidence
        .pointer("/workload/iterations")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_default();
    let workload_summary = format!(
        "metric_shapes={metric_shapes} log_shapes={log_shapes} measured_iterations={}",
        (metric_shapes + log_shapes) as u64 * iterations
    );
    let workload_marker = Regex::new(r"<!--\s*query-release-workload-summary:\s*(.*?)\s*-->")?;
    let actual_workload = workload_marker
        .captures(&content)
        .map(|captures| captures[1].trim().to_owned());
    if actual_workload.as_deref() != Some(workload_summary.as_str()) {
        errors.push(format!(
            "{relative}: workload summary differs; expected {workload_summary:?}, got {actual_workload:?}"
        ));
    }

    let baseline_path = root.join("docs/evidence/2026-08-04_query_baseline.json");
    if !baseline_path.is_file() {
        errors.push(format!("{relative}: missing Session 0 comparison evidence"));
    } else {
        let baseline: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(baseline_path)?)?;
        for (signal, query) in [
            ("metrics", "narrow"),
            ("metrics", "wide"),
            ("logs", "narrow"),
            ("logs", "wide"),
        ] {
            for (label, document) in [("Session 0", &baseline), ("final", &evidence)] {
                let Some(triplet) = latency_triplet(document, signal, query) else {
                    errors.push(format!(
                        "{relative}: {label} evidence lacks {signal}/{query} p50/p95/p99"
                    ));
                    continue;
                };
                if !content.contains(&triplet) {
                    errors.push(format!(
                        "{relative}: {label} {signal}/{query} latency {triplet} is not reported"
                    ));
                }
            }
        }
    }

    Ok(errors)
}

pub(crate) fn validate(root: &Path) -> Result<Vec<String>> {
    let id_pattern = Regex::new(r"^(?:PQL-[SORFH]\d{2}|MQL-\d{2}|LQL-[FPSQ]\d{2})$")?;
    let recipe_link = Regex::new(r"\[[^\]]*\]\((QUERY_SQL_EQUIVALENTS\.md#[^)]+)\)")?;
    let equivalent_id = Regex::new(r"`((?:PQL|MQL|LQL)-[A-Z]?\d{2})`")?;
    let mut errors = Vec::new();
    let mut rows = Vec::new();
    for relative in MATRICES {
        let path = root.join(relative);
        if !path.exists() {
            errors.push(format!("missing matrix {relative}"));
            continue;
        }
        let (parsed, mut parse_errors) = parse_matrix(&path)?;
        rows.extend(parsed);
        errors.append(&mut parse_errors);
    }

    let mut identifiers: BTreeMap<String, MatrixRow> = BTreeMap::new();
    for row in &rows {
        let relative = row.source.strip_prefix(root).unwrap_or(&row.source);
        let location = format!("{}:{}", relative.display(), row.line);
        if !id_pattern.is_match(&row.identifier) {
            errors.push(format!("{location}: illegal row ID {}", row.identifier));
        }
        if let Some(first) = identifiers.get(&row.identifier) {
            errors.push(format!(
                "{location}: duplicate row ID {} (first at {}:{})",
                row.identifier,
                first.source.display(),
                first.line
            ));
        }
        identifiers.insert(row.identifier.clone(), row.clone());
        if !LEGAL_STATUSES.contains(&row.status.as_str()) {
            errors.push(format!("{location}: illegal status {}", row.status));
        }
        if !LEGAL_TARGETS.contains(&row.target.as_str()) {
            errors.push(format!("{location}: illegal target {}", row.target));
        }
        if !LEGAL_PRIORITIES.contains(&row.priority.as_str()) {
            errors.push(format!("{location}: illegal priority {}", row.priority));
        }
        if row.target == "DEFER" && row.status != "deferred" {
            errors.push(format!(
                "{location}: DEFER target must have deferred status"
            ));
        }
        if row.status == "deferred" && row.target != "DEFER" {
            errors.push(format!(
                "{location}: deferred status must have DEFER target"
            ));
        }
    }

    let references_path = root.join("docs/QUERY_TEST_REFERENCES.md");
    let (references, mut reference_errors) = if references_path.exists() {
        parse_test_references(&references_path)?
    } else {
        errors.push("missing docs/QUERY_TEST_REFERENCES.md".to_owned());
        (BTreeMap::new(), Vec::new())
    };
    errors.append(&mut reference_errors);
    let shipped: BTreeSet<_> = rows
        .iter()
        .filter(|row| row.status == "shipped")
        .map(|row| row.identifier.clone())
        .collect();
    for identifier in shipped.difference(&references.keys().cloned().collect()) {
        errors.push(format!("shipped row {identifier} has no test reference"));
    }
    for identifier in references
        .keys()
        .filter(|identifier| !shipped.contains(*identifier))
    {
        errors.push(format!(
            "test reference {identifier} does not name a shipped row"
        ));
    }
    for (identifier, (relative, symbol)) in &references {
        let path = root.join(relative);
        if !path.is_file() {
            errors.push(format!(
                "test reference {identifier} has missing path {relative}"
            ));
        } else if !fs::read_to_string(&path)?.contains(symbol) {
            errors.push(format!(
                "test reference {identifier} has missing symbol {symbol} in {relative}"
            ));
        }
    }

    for row in &rows {
        let has_recipe = recipe_link.captures_iter(&row.raw).next().is_some();
        if row.status == "shipped"
            && (row.target == "SQL" || row.foundation.contains("SQL"))
            && !has_recipe
        {
            errors.push(format!(
                "shipped SQL-founded row {} has no executable recipe link",
                row.identifier
            ));
        }
    }

    for (relative, prefixes) in [
        (
            "servers/crates/timeless-metrics-api/README.md",
            &["PQL-", "MQL-"][..],
        ),
        ("servers/crates/timeless-logs-api/README.md", &["LQL-"][..]),
    ] {
        let path = root.join(relative);
        if !path.exists() {
            errors.push(format!("missing server documentation {relative}"));
            continue;
        }
        let (actual, mut marker_errors) = shipped_marker(&path)?;
        errors.append(&mut marker_errors);
        let expected: BTreeSet<_> = rows
            .iter()
            .filter(|row| {
                row.status == "shipped"
                    && row.target == "API"
                    && prefixes
                        .iter()
                        .any(|prefix| row.identifier.starts_with(prefix))
            })
            .map(|row| row.identifier.clone())
            .collect();
        if actual != expected {
            errors.push(format!(
                "{relative}: shipped marker mismatch; expected {expected:?}, got {actual:?}"
            ));
        }
    }

    let equivalents = root.join("docs/QUERY_SQL_EQUIVALENTS.md");
    if equivalents.exists() {
        for captures in equivalent_id.captures_iter(&fs::read_to_string(&equivalents)?) {
            let identifier = &captures[1];
            if !identifiers.contains_key(identifier) {
                errors.push(format!(
                    "docs/QUERY_SQL_EQUIVALENTS.md: unknown matrix row {identifier}"
                ));
            }
        }
    } else {
        errors.push("missing docs/QUERY_SQL_EQUIVALENTS.md".to_owned());
    }
    errors.extend(validate_local_links(root)?);
    errors.extend(validate_signal_storage_boundaries(root)?);
    errors.extend(validate_public_sql_inventory(root)?);
    errors.extend(validate_public_server_routes(root)?);
    errors.extend(validate_public_server_environment(root)?);
    errors.extend(validate_public_compatibility_versions(root)?);
    errors.extend(validate_public_artifact_inventory(root)?);
    errors.extend(validate_public_embedding_contract(root)?);
    errors.extend(validate_markdown_table_structure(root)?);
    errors.extend(validate_query_storage_findings(root)?);
    errors.extend(validate_query_release_report(root, &rows)?);
    errors.extend(validate_canonical_documentation_wording(root)?);
    Ok(errors)
}

pub(crate) fn run(root: &Path) -> Result<()> {
    let errors = validate(root)?;
    if !errors.is_empty() {
        for error in errors {
            eprintln!("query-contract: {error}");
        }
        bail!("query contract validation failed");
    }
    println!("query contracts: ok");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const PROM: &str = "# Prom matrix\n\n\
| ID | construct | Rust now | Elixir | foundation | target | priority |\n\
|---|---|---|---|---|---|---|\n\
| `PQL-S01` | selector ([SQL](QUERY_SQL_EQUIVALENTS.md#sql-prom-001)) | shipped | yes | `SQL` | `API` | P0 |\n";
    const LOGS: &str = "# Logs matrix\n\n\
| ID | construct | Rust now | foundation | target | priority |\n\
|---|---|---|---|---|---|\n\
| `LQL-F01` | filter | missing | `ROWS` | `API` | P0 |\n";

    fn fixture() -> TempDir {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path();
        for relative in [
            "docs",
            "tests",
            "servers/crates/timeless-metrics-api",
            "servers/crates/timeless-logs-api",
        ] {
            fs::create_dir_all(root.join(relative)).unwrap();
        }
        fs::write(
            root.join("README.md"),
            "# Root\n\n[Matrix](docs/PROMQL_FEATURE_MATRIX.md)\n",
        )
        .unwrap();
        fs::write(root.join("docs/PROMQL_FEATURE_MATRIX.md"), PROM).unwrap();
        fs::write(root.join("docs/LOGSQL_FEATURE_MATRIX.md"), LOGS).unwrap();
        fs::write(
            root.join("docs/QUERY_SQL_EQUIVALENTS.md"),
            "# SQL\n\n## SQL-PROM-001\n",
        )
        .unwrap();
        fs::write(
            root.join("docs/QUERY_TEST_REFERENCES.md"),
            "# Tests\n\n| ID | test path | test symbol | coverage |\n\
             |---|---|---|---|\n\
             | `PQL-S01` | `tests/oracle.rs` | `selector_oracle` | fixture |\n",
        )
        .unwrap();
        fs::write(root.join("tests/oracle.rs"), "fn selector_oracle() {}\n").unwrap();
        fs::write(
            root.join("servers/crates/timeless-metrics-api/README.md"),
            "# Metrics\n\n<!-- query-contract-shipped: PQL-S01 -->\n",
        )
        .unwrap();
        fs::write(
            root.join("servers/crates/timeless-logs-api/README.md"),
            "# Logs\n\n<!-- query-contract-shipped: -->\n",
        )
        .unwrap();
        assert_eq!(validate(root).unwrap(), Vec::<String>::new());
        temporary
    }

    fn assert_invalid(root: &Path, needle: &str) {
        let errors = validate(root).unwrap();
        assert!(
            errors.iter().any(|error| error.contains(needle)),
            "{errors:?}"
        );
    }

    #[test]
    fn checked_in_contracts_are_valid() {
        let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let root = manifest.parent().unwrap().parent().unwrap();
        assert_eq!(validate(root).unwrap(), Vec::<String>::new());
    }

    #[test]
    fn final_report_is_required_when_the_release_plan_exists() {
        let fixture = fixture();
        fs::write(
            fixture
                .path()
                .join("docs/2026-08-04_query_surface_implementation_plan.md"),
            "# Release plan\n",
        )
        .unwrap();
        assert_invalid(fixture.path(), "missing docs/QUERY_RELEASE_REPORT.md");
    }

    #[test]
    fn final_report_matrix_summary_is_derived_from_the_matrices() {
        let fixture = fixture();
        fs::write(
            fixture
                .path()
                .join("docs/2026-08-04_query_surface_implementation_plan.md"),
            "# Release plan\n",
        )
        .unwrap();
        fs::write(
            fixture.path().join("docs/QUERY_RELEASE_REPORT.md"),
            "# Report\n\n<!-- query-release-matrix-summary: PQL shipped=999 -->\n",
        )
        .unwrap();
        assert_invalid(fixture.path(), "matrix summary differs");
    }

    #[test]
    fn final_report_requires_owned_fault_evidence() {
        let fixture = fixture();
        fs::write(
            fixture
                .path()
                .join("docs/2026-08-04_query_surface_implementation_plan.md"),
            "# Release plan\n",
        )
        .unwrap();
        fs::write(
            fixture.path().join("docs/QUERY_RELEASE_REPORT.md"),
            "# Report\n\n<!-- query-release-matrix-summary: LQL missing=1; PQL shipped=1 -->\n",
        )
        .unwrap();
        assert_invalid(
            fixture.path(),
            "missing query-release-fault-evidence marker",
        );
    }

    #[test]
    fn storage_findings_require_contiguous_structured_terminal_rows() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path();
        fs::create_dir(root.join("docs")).unwrap();
        let mut document = String::from("# Findings\n\n");
        for identifier in 1..=7 {
            document.push_str(&format!(
                "| `QSF-{identifier:03}` | baseline behavior | consequence |\n"
            ));
        }
        document.push_str(
            "| `QSF-008` | date | row | an unescaped | pipe | expected | evidence | disposition | open |\n",
        );
        document.push_str(
            "| `QSF-010` | date | row | observation | expected | evidence | disposition | accepted |\n",
        );
        fs::write(root.join("docs/QUERY_STORAGE_FINDINGS.md"), document).unwrap();

        let errors = validate_query_storage_findings(root).unwrap();
        assert!(errors
            .iter()
            .any(|error| error.contains("QSF-008 has 10 unescaped table separators")));
        assert!(errors
            .iter()
            .any(|error| error.contains("illegal terminal status")));
        assert!(errors
            .iter()
            .any(|error| error.contains("expected QSF-009, got QSF-010")));
    }

    #[test]
    fn markdown_tables_reject_unescaped_extra_columns_outside_fences() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path();
        fs::create_dir(root.join("docs")).unwrap();
        fs::write(
            root.join("docs/tables.md"),
            "# Tables\n\n\
             | first | second |\n\
             |---|---|\n\
             | value | an unescaped | pipeline |\n\n\
             ```text\n\
             | this | is | literal | fenced | text |\n\
             ```\n",
        )
        .unwrap();

        let errors = validate_markdown_table_structure(root).unwrap();
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert!(errors[0].contains("tables.md:5"));
        assert!(errors[0].contains("has 4 unescaped separators; expected 3"));
    }

    #[test]
    fn duplicate_ids_fail() {
        let fixture = fixture();
        let path = fixture.path().join("docs/PROMQL_FEATURE_MATRIX.md");
        let mut content = fs::read_to_string(&path).unwrap();
        content.push_str(PROM.lines().last().unwrap());
        content.push('\n');
        fs::write(&path, content).unwrap();
        assert_invalid(fixture.path(), "duplicate row ID PQL-S01");
    }

    #[test]
    fn illegal_status_owner_and_priority_fail() {
        let fixture = fixture();
        let path = fixture.path().join("docs/PROMQL_FEATURE_MATRIX.md");
        fs::write(
            &path,
            PROM.replace("shipped", "almost")
                .replace("`API`", "`BEAM`")
                .replace("P0 |", "NOW |"),
        )
        .unwrap();
        let errors = validate(fixture.path()).unwrap();
        for needle in ["illegal status", "illegal target", "illegal priority"] {
            assert!(
                errors.iter().any(|error| error.contains(needle)),
                "{errors:?}"
            );
        }
    }

    #[test]
    fn missing_shipped_test_reference_fails() {
        let fixture = fixture();
        fs::write(
            fixture.path().join("docs/QUERY_TEST_REFERENCES.md"),
            "# Tests\n",
        )
        .unwrap();
        assert_invalid(fixture.path(), "shipped row PQL-S01 has no test reference");
    }

    #[test]
    fn missing_sql_recipe_anchor_fails() {
        let fixture = fixture();
        fs::write(
            fixture.path().join("docs/QUERY_SQL_EQUIVALENTS.md"),
            "# SQL\n",
        )
        .unwrap();
        assert_invalid(fixture.path(), "missing anchor");
    }

    #[test]
    fn broken_local_link_fails() {
        let fixture = fixture();
        fs::write(
            fixture.path().join("README.md"),
            "# Root\n\n[Gone](docs/GONE.md)\n",
        )
        .unwrap();
        assert_invalid(fixture.path(), "broken local link");
    }

    #[test]
    fn repository_documentation_scan_excludes_untracked_drafts() {
        let fixture = fixture();
        fs::write(
            fixture.path().join("docs/tracked.md"),
            "# Tracked\n\n[Root](../README.md)\n",
        )
        .unwrap();
        fs::write(
            fixture.path().join("docs/untracked.md"),
            "# Draft\n\n[Missing](private-draft-target.md)\n",
        )
        .unwrap();
        let status = Command::new("git")
            .args(["init", "--quiet"])
            .current_dir(fixture.path())
            .status()
            .unwrap();
        assert!(status.success());
        let status = Command::new("git")
            .args(["add", "README.md", "docs/tracked.md"])
            .current_dir(fixture.path())
            .status()
            .unwrap();
        assert!(status.success());
        let files = markdown_files(fixture.path()).unwrap();
        assert!(files.ends_with(&[
            fixture.path().join("README.md"),
            fixture.path().join("docs/tracked.md")
        ]));
        assert!(!files.contains(&fixture.path().join("docs/untracked.md")));
    }

    #[test]
    fn server_matrix_disagreement_fails() {
        let fixture = fixture();
        fs::write(
            fixture
                .path()
                .join("servers/crates/timeless-metrics-api/README.md"),
            "# Metrics\n\n<!-- query-contract-shipped: -->\n",
        )
        .unwrap();
        assert_invalid(fixture.path(), "shipped marker mismatch");
    }

    #[test]
    fn signal_servers_private_shadow_table_access_fails() {
        let fixture = fixture();
        for (signal, source) in [
            (
                "metrics",
                "fn bad(table: &str) { let _ = format!(\"{}_chunks\", table); }\n",
            ),
            ("logs", "const BAD: &str = \"SELECT * FROM logs_blocks\";\n"),
            (
                "traces",
                "const BAD: &str = \"SELECT * FROM traces_trace_blocks\";\n",
            ),
        ] {
            let directory = fixture
                .path()
                .join(format!("servers/crates/timeless-{signal}-api/src"));
            fs::create_dir_all(&directory).unwrap();
            fs::write(directory.join("storage.rs"), source).unwrap();
        }
        let errors = validate_signal_storage_boundaries(fixture.path()).unwrap();
        assert_eq!(errors.len(), 3, "{errors:#?}");
        assert!(errors.iter().any(|error| error.contains("metrics server")));
        assert!(errors.iter().any(|error| error.contains("logs server")));
        assert!(errors.iter().any(|error| error.contains("traces server")));
    }

    #[test]
    fn signal_servers_public_stats_access_passes() {
        let fixture = fixture();
        for signal in ["metrics", "logs", "traces"] {
            let directory = fixture
                .path()
                .join(format!("servers/crates/timeless-{signal}-api/src"));
            fs::create_dir_all(&directory).unwrap();
            fs::write(
                directory.join("storage.rs"),
                format!(
                    "const SQL: &str = \"SELECT key,value FROM timeless_stats('{signal}')\";\n"
                ),
            )
            .unwrap();
        }
        let errors = validate_signal_storage_boundaries(fixture.path()).unwrap();
        assert!(errors.is_empty(), "{errors:#?}");
    }

    #[test]
    fn public_sql_inventory_must_match_registered_symbols() {
        let fixture = fixture();
        let source = fixture.path().join("crates/timeless-ext/src");
        fs::create_dir_all(&source).unwrap();
        fs::write(
            source.join("registration.rs"),
            "db.create_module(c\"timeless_one\", &ONE, None::<()>)?;\n\
             db.create_scalar_function(\"timeless_two\", 0, flags, body)?;\n",
        )
        .unwrap();
        fs::write(
            fixture.path().join("docs/SQL_API_REFERENCE.md"),
            "# SQL\n\n<!-- public-sql-symbols:start -->\n\n\
             | SQL symbol | kind |\n|---|---|\n\
             | `timeless_one` | module |\n\n\
             <!-- public-sql-symbols:end -->\n",
        )
        .unwrap();
        assert_invalid(fixture.path(), "public SQL symbol timeless_two");
    }

    #[test]
    fn public_server_route_inventory_must_match_source_methods_and_paths() {
        let fixture = fixture();
        let source = fixture
            .path()
            .join("servers/crates/timeless-metrics-api/src");
        fs::create_dir_all(&source).unwrap();
        fs::write(
            source.join("api.rs"),
            "Router::new()\n  .route(\"/live\", get(live))\n  .route(\"/query\", get(query).post(query));\n",
        )
        .unwrap();
        fs::write(
            fixture.path().join("docs/SERVER_API_REFERENCE.md"),
            "# Server API\n\n<!-- public-server-routes:start -->\n\n\
             | Signal | Methods | Path |\n|---|---|---|\n\
             | `metrics` | `GET` | `/live` |\n\
             | `metrics` | `GET, PUT` | `/query` |\n\n\
             <!-- public-server-routes:end -->\n",
        )
        .unwrap();
        assert_invalid(fixture.path(), "route /query methods differ");

        fs::write(
            fixture.path().join("docs/SERVER_API_REFERENCE.md"),
            "# Server API\n\n<!-- public-server-routes:start -->\n\n\
             | Signal | Methods | Path |\n|---|---|---|\n\
             | `metrics` | `GET, POST` | `/query` |\n\
             | `metrics` | `GET` | `/not-registered` |\n\n\
             <!-- public-server-routes:end -->\n",
        )
        .unwrap();
        let errors = validate(fixture.path()).unwrap();
        assert!(errors
            .iter()
            .any(|error| error.contains("route /live has no inventory row")));
        assert!(errors
            .iter()
            .any(|error| error.contains("/not-registered is not registered")));
    }

    #[test]
    fn public_server_environment_inventory_must_match_production_source() {
        let fixture = fixture();
        let source = fixture
            .path()
            .join("servers/crates/timeless-metrics-api/src");
        fs::create_dir_all(&source).unwrap();
        fs::write(
            source.join("main.rs"),
            "let _ = std::env::var(\"TIMELESS_ONE\");\n#[cfg(test)]\nconst TEST: &str = \"TIMELESS_TEST_ONLY\";\n",
        )
        .unwrap();
        fs::write(
            fixture.path().join("docs/SERVER_API_REFERENCE.md"),
            "# Server API\n\n<!-- public-server-environment:start -->\n\n\
             | Variable | Default |\n|---|---|\n\
             | `TIMELESS_TWO` | unset |\n\n\
             <!-- public-server-environment:end -->\n",
        )
        .unwrap();
        let errors = validate(fixture.path()).unwrap();
        assert!(errors.iter().any(|error| error.contains("TIMELESS_ONE")));
        assert!(errors.iter().any(|error| error.contains("TIMELESS_TWO")));
        assert!(!errors
            .iter()
            .any(|error| error.contains("TIMELESS_TEST_ONLY")));
    }

    #[test]
    fn public_compatibility_inventory_must_match_both_workspaces_and_floors() {
        let fixture = fixture();
        for relative in [
            "crates/timeless-ext/src",
            "servers/crates/timeless-api-common/src",
        ] {
            fs::create_dir_all(fixture.path().join(relative)).unwrap();
        }
        fs::write(
            fixture.path().join("Cargo.toml"),
            "[workspace.package]\nversion = \"0.4.0\"\n",
        )
        .unwrap();
        fs::write(
            fixture.path().join("servers/Cargo.toml"),
            "[workspace.package]\nversion = \"0.4.0\"\n",
        )
        .unwrap();
        fs::write(
            fixture
                .path()
                .join("crates/timeless-ext/src/capabilities.rs"),
            "const DATA_ABI: u64 = 1;\nfn value() { let _ = serde_json::json!({\"sql_surface_version\": 1, \"minimum_server_version\": \"0.4.0\"}); }\n",
        )
        .unwrap();
        fs::write(
            fixture
                .path()
                .join("servers/crates/timeless-api-common/src/lib.rs"),
            "pub const DATA_SCHEMA_VERSION: i64 = 1;\n\
             pub const REQUIRED_EXTENSION_DATA_ABI: u64 = 1;\n\
             pub const MINIMUM_EXTENSION_VERSION: &str = \"0.4.0\";\n",
        )
        .unwrap();
        fs::write(
            fixture.path().join("docs/COMPATIBILITY.md"),
            "# Compatibility\n\n<!-- public-compatibility-versions:start -->\n\n\
             | Contract key | Current value | Meaning |\n|---|---|---|\n\
             | `extension_workspace` | `0.3.0` | stale |\n\
             | `server_workspace` | `0.4.0` | current |\n\
             | `extension_data_abi` | `1` | current |\n\
             | `sql_surface_version` | `1` | current |\n\
             | `extension_minimum_server` | `0.4.0` | current |\n\
             | `server_data_schema` | `1` | current |\n\
             | `server_required_data_abi` | `1` | current |\n\
             | `server_minimum_extension` | `0.4.0` | current |\n\n\
             <!-- public-compatibility-versions:end -->\n",
        )
        .unwrap();
        fs::write(
            fixture.path().join("CHANGELOG.md"),
            "# Changelog\n\n<!-- release-target: 0.3.0 -->\n",
        )
        .unwrap();
        let errors = validate(fixture.path()).unwrap();
        assert!(errors
            .iter()
            .any(|error| error.contains("extension_workspace differs")));
        assert!(errors.iter().any(|error| error.contains("release target")));
    }

    #[test]
    fn public_artifact_inventory_must_match_packager_targets_and_payloads() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path();
        fs::create_dir_all(root.join("tools")).unwrap();
        fs::create_dir_all(root.join("docs")).unwrap();
        fs::create_dir_all(root.join("tools/release-tool")).unwrap();
        fs::write(
            root.join("tools/release-tool/artifact-inventory.json"),
            r#"{
  "schema": 1,
  "binaries": ["timeless-metrics-api", "timeless-logs-api", "timeless-traces-api"],
  "targets": [
    {"triple": "x86_64-unknown-linux-gnu", "extension_suffix": "so", "platform": "linux"},
    {"triple": "aarch64-apple-darwin", "extension_suffix": "dylib", "platform": "macos"}
  ],
  "fixed_files": [
    "install.sh",
    "uninstall.sh",
    "licenses/timeless-libsql-MIT.txt",
    "SBOM.spdx.json",
    "THIRD_PARTY_LICENSES.txt",
    "artifact-manifest.json",
    "SHA256SUMS"
  ]
}"#,
        )
        .unwrap();
        let document = r#"# Artifacts

<!-- public-artifact-targets:start -->

| Rust target | Platform | Extension file |
|---|---|---|
| `x86_64-unknown-linux-gnu` | Linux | `lib/libtimeless_ext.so` |
| `aarch64-apple-darwin` | macOS | `lib/libtimeless_ext.dylib` |

<!-- public-artifact-targets:end -->

<!-- public-artifact-files:start -->

| Archive path | Contract |
|---|---|
| `bin/timeless-metrics-api` | binary |
| `bin/timeless-logs-api` | binary |
| `bin/timeless-traces-api` | binary |
| `lib/libtimeless_ext.so` or `lib/libtimeless_ext.dylib` | extension |
| `install.sh` | installer |
| `uninstall.sh` | remover |
| `licenses/timeless-libsql-MIT.txt` | license |
| `SBOM.spdx.json` | sbom |
| `THIRD_PARTY_LICENSES.txt` | notices |
| `artifact-manifest.json` | manifest |
| `SHA256SUMS` | checksums |

<!-- public-artifact-files:end -->
"#;
        let path = root.join("docs/ARTIFACTS.md");
        fs::write(&path, document).unwrap();
        assert!(validate_public_artifact_inventory(root).unwrap().is_empty());

        fs::write(
            &path,
            document.replace("`bin/timeless-traces-api`", "`bin/timeless-generic-api`"),
        )
        .unwrap();
        let errors = validate_public_artifact_inventory(root).unwrap();
        assert!(errors
            .iter()
            .any(|error| error.contains("archive file inventory differs")));
    }

    #[test]
    fn public_embedding_contract_rejects_compatibility_spike_as_smoke() {
        let temporary = TempDir::new().unwrap();
        let root = temporary.path();
        for relative in [
            "crates/timeless-ext/examples",
            "crates/timeless-ext/src",
            "tools/libsql-check/src",
            "docs",
        ] {
            fs::create_dir_all(root.join(relative)).unwrap();
        }
        fs::write(
            root.join("crates/timeless-ext/Cargo.toml"),
            r#"[[example]]
name = "embedded"
required-features = ["embedded"]
[dependencies]
rusqlite = { version = "0.40.1", features = ["functions", "vtab"] }
[features]
default = ["entrypoints"]
entrypoints = ["rusqlite/loadable_extension"]
embedded = []
"#,
        )
        .unwrap();
        fs::write(
            root.join("tools/libsql-check/Cargo.lock"),
            "[[package]]\nname = \"libsql\"\nversion = \"0.9.30\"\n",
        )
        .unwrap();
        fs::write(
            root.join("crates/timeless-ext/src/lib.rs"),
            "#[cfg(feature = \"entrypoints\")] fn load() {}\n\
             #[cfg(feature = \"embedded\")] fn linked() {}\n\
             pub fn register_telemetry() {}\n\
             pub fn register_dbhealth() {}\n",
        )
        .unwrap();
        let smoke = "timeless_metrics timeless_logs timeless_traces \
                     status_description events resource instrumentation_scope";
        fs::write(root.join("crates/timeless-ext/examples/embedded.rs"), smoke).unwrap();
        let gate = format!(
            "{smoke}\nBuilder::new_local(&database_path)\nBuilder::new_local(&database_path)\n"
        );
        let gate_path = root.join("tools/libsql-check/src/main.rs");
        fs::write(&gate_path, &gate).unwrap();
        fs::write(
            root.join("docs/EMBEDDED_RUST.md"),
            r#"# Embedded

<!-- public-embedding-contract:start -->

| Contract key | Current value |
|---|---|
| `timeless_ext_embedded_feature` | `embedded` |
| `timeless_ext_loadable_feature` | `entrypoints` |
| `rusqlite_version` | `0.40.1` |
| `direct_libsql_gate_version` | `0.9.30` |
| `static_example` | `crates/timeless-ext/examples/embedded.rs` |
| `dynamic_libsql_gate` | `tools/libsql-check/src/main.rs` |

<!-- public-embedding-contract:end -->
"#,
        )
        .unwrap();
        assert!(validate_public_embedding_contract(root).unwrap().is_empty());

        fs::write(
            &gate_path,
            format!("{gate}CREATE VIRTUAL TABLE spike USING timeless_spike;\n"),
        )
        .unwrap();
        let errors = validate_public_embedding_contract(root).unwrap();
        assert!(errors.iter().any(
            |error| error.contains("production embedding smoke must not create timeless_spike")
        ));
    }
}
