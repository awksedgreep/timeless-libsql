use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

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
    let mut files = vec![root.join("README.md")];
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

fn validate_logs_storage_boundary(root: &Path) -> Result<Vec<String>> {
    let relative = "servers/crates/timeless-logs-api/src/storage.rs";
    let path = root.join(relative);
    if !path.is_file() {
        return Ok(Vec::new());
    }
    let source = fs::read_to_string(path)?;
    Ok(["logs_blocks", "logs_terms", "logs_meta"]
        .into_iter()
        .filter(|name| source.contains(name))
        .map(|name| {
            format!(
                "{relative}: server references private extension shadow table {name}; use a public virtual table or scalar"
            )
        })
        .collect())
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
    errors.extend(validate_logs_storage_boundary(root)?);
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
    fn logs_server_private_shadow_table_access_fails() {
        let fixture = fixture();
        let source = fixture.path().join("servers/crates/timeless-logs-api/src");
        fs::create_dir_all(&source).unwrap();
        fs::write(
            source.join("storage.rs"),
            "const BAD: &str = \"SELECT * FROM logs_blocks\";\n",
        )
        .unwrap();
        assert_invalid(
            fixture.path(),
            "server references private extension shadow table logs_blocks",
        );
    }
}
