//! Machine-readable binary/extension capability handshake.
//!
//! This is intentionally an ordinary deterministic SQL scalar so embedded
//! SQLite/libSQL users and the three Timeless servers negotiate the exact same
//! public surface before creating or opening a telemetry virtual table.

use std::sync::OnceLock;

use rusqlite::functions::FunctionFlags;
use rusqlite::{Connection, Result};
use serde_json::json;

const DATA_ABI: u64 = 1;

pub(crate) fn register(db: &Connection) -> Result<()> {
    db.create_scalar_function(
        "timeless_capabilities",
        0,
        FunctionFlags::SQLITE_UTF8
            | FunctionFlags::SQLITE_DETERMINISTIC
            | FunctionFlags::SQLITE_INNOCUOUS,
        |_| Ok(document()),
    )
}

fn document() -> &'static str {
    static DOCUMENT: OnceLock<String> = OnceLock::new();
    DOCUMENT.get_or_init(|| {
        json!({
            "extension_version": env!("CARGO_PKG_VERSION"),
            "data_abi": DATA_ABI,
            "minimum_server_version": "0.3.0",
            "build": {
                "commit": env!("TIMELESS_BUILD_COMMIT_RESOLVED"),
                "target": env!("TIMELESS_BUILD_TARGET"),
                "profile": env!("TIMELESS_BUILD_PROFILE")
            },
            "signals": {
                "metrics": {
                    "module": "timeless_metrics",
                    "batches": ["named-v0", "resolved-v1"],
                    "timestamp_unit": "seconds",
                    "authoritative_batch_points_per_series": 4096,
                    "rollups": true
                },
                "logs": {
                    "module": "timeless_logs",
                    "batches": ["flat-v0", "rich-v1"],
                    "timestamp_units": ["milliseconds", "microseconds"],
                    "authoritative_batch_entries": 8192,
                    "exact_severity": true,
                    "typed_metadata": true
                },
                "traces": {
                    "module": "timeless_traces",
                    "batches": ["span-v0", "rich-span-v1"],
                    "timestamp_unit": "nanoseconds",
                    "authoritative_batch_spans": 8192,
                    "rich_span_fidelity": true
                }
            },
            "query_surfaces": {
                "timeless_raw_frame": {
                    "format": "TRF1",
                    "max_work_points": true
                },
                "timeless_window_batches": {
                    "format": "TWB1",
                    "max_work_points": true
                }
            }
        })
        .to_string()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn document_is_stable_and_names_every_release_signal() {
        let value: serde_json::Value = serde_json::from_str(document()).unwrap();
        assert_eq!(value["data_abi"], 1);
        assert_eq!(value["signals"]["metrics"]["rollups"], true);
        assert_eq!(value["signals"]["logs"]["typed_metadata"], true);
        assert_eq!(value["signals"]["traces"]["rich_span_fidelity"], true);
        assert_eq!(
            value["query_surfaces"]["timeless_raw_frame"]["max_work_points"],
            true
        );
        assert_eq!(
            value["query_surfaces"]["timeless_window_batches"]["max_work_points"],
            true
        );
    }
}
