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
            "sql_surface_version": 1,
            "minimum_server_version": "0.5.0",
            "build": {
                "commit": env!("TIMELESS_BUILD_COMMIT_RESOLVED"),
                "target": env!("TIMELESS_BUILD_TARGET"),
                "profile": env!("TIMELESS_BUILD_PROFILE")
            },
            "signals": {
                "metrics": {
                    "module": "timeless_metrics",
                    "batches": ["named-v0", "resolved-v1"],
                    "sample_types": ["float64"],
                    "native_histograms": false,
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
                    "batches": ["span-v0", "rich-span-v1", "rich-span-v2"],
                    "timestamp_unit": "nanoseconds",
                    "authoritative_batch_spans": 8192,
                    "rich_span_fidelity": true,
                    "rich_span_fidelity_version": 2,
                    "rich_span_fields": [
                        "links",
                        "trace_state",
                        "trace_flags",
                        "dropped_attributes_count",
                        "dropped_events_count",
                        "dropped_links_count",
                        "resource_schema_url",
                        "scope_schema_url",
                        "resource_dropped_attributes_count",
                        "scope_dropped_attributes_count"
                    ],
                    "projection_decode": {
                        "version": 1,
                        "sqlite_col_used": true,
                        "predicate_first": true,
                        "legacy_full_decode_fallback": true
                    },
                    "duration_block_pruning": {
                        "version": 1,
                        "inclusive_bounds": true,
                        "legacy_decode_fallback": true,
                        "optimize_backfill": true
                    },
                    "attribute_equality": {
                        "version": 1,
                        "configuration": "attribute_indexes",
                        "hidden_input": "attribute_filter",
                        "scopes": ["span", "resource", "scope"],
                        "path": "RFC6901",
                        "typed_scalars": true,
                        "max_fields": 8,
                        "legacy_decode_fallback": true
                    }
                }
            },
            "query_surfaces": {
                "timeless_raw_batches": {
                    "format": "raw-series-v0",
                    "versioned": false,
                    "preferred_wide_format": "TRF1"
                },
                "timeless_raw_frame": {
                    "format": "TRF1",
                    "max_work_points": true
                },
                "timeless_window_batches": {
                    "format": "TWB1",
                    "max_work_points": true
                },
                "timeless_rollup_batches": {
                    "format": "TRB1"
                },
                "timeless_aggregate_frame": {
                    "format": "TAF1"
                },
                "timeless_latest_frame": {
                    "format": "TLF1"
                },
                "timeless_logs": {
                    "max_work_entries": true
                },
                "timeless_log_count": {
                    "max_work_entries": true
                },
                "timeless_log_values": {
                    "max_work_entries": true
                },
                "timeless_log_query_stats": {
                    "request_local": true,
                    "same_connection": true,
                    "single_use": true
                }
            },
            "sql_surfaces": {
                "scalar_functions": ["timeless_capabilities"],
                "storage_modules": [
                    "timeless_metrics",
                    "timeless_logs",
                    "timeless_traces"
                ],
                "query_modules": [
                    "timeless_aggregate",
                    "timeless_aggregate_frame",
                    "timeless_grid",
                    "timeless_label_values",
                    "timeless_latest",
                    "timeless_latest_frame",
                    "timeless_log_buckets",
                    "timeless_log_count",
                    "timeless_log_query_stats",
                    "timeless_log_values",
                    "timeless_raw",
                    "timeless_raw_batches",
                    "timeless_raw_frame",
                    "timeless_rollup",
                    "timeless_rollup_batches",
                    "timeless_series",
                    "timeless_stats",
                    "timeless_trace_buckets",
                    "timeless_trace_operations",
                    "timeless_trace_services",
                    "timeless_window",
                    "timeless_window_batches"
                ]
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
        assert_ne!(
            env!("CARGO_PKG_VERSION"),
            "0.3.0",
            "the tagged v0.3.0 artifact predates the capability handshake"
        );
        assert!(
            env!("CARGO_PKG_VERSION").starts_with("0.5."),
            "the current extension must remain on the documented 0.5 compatibility line"
        );
        assert_eq!(
            value["minimum_server_version"], "0.5.0",
            "compatible 0.5 patch releases retain the 0.5.0 server floor"
        );
        assert_eq!(value["data_abi"], 1);
        assert_eq!(value["sql_surface_version"], 1);
        assert_eq!(value["signals"]["metrics"]["rollups"], true);
        assert_eq!(
            value["signals"]["metrics"]["sample_types"],
            json!(["float64"])
        );
        assert_eq!(value["signals"]["metrics"]["native_histograms"], false);
        assert_eq!(value["signals"]["logs"]["typed_metadata"], true);
        assert_eq!(value["signals"]["traces"]["rich_span_fidelity"], true);
        assert_eq!(value["signals"]["traces"]["rich_span_fidelity_version"], 2);
        assert!(value["signals"]["traces"]["batches"]
            .as_array()
            .unwrap()
            .contains(&json!("rich-span-v2")));
        assert_eq!(
            value["signals"]["traces"]["projection_decode"]["version"],
            1
        );
        assert_eq!(
            value["signals"]["traces"]["projection_decode"]["predicate_first"],
            true
        );
        assert_eq!(
            value["signals"]["traces"]["projection_decode"]["legacy_full_decode_fallback"],
            true
        );
        assert_eq!(
            value["signals"]["traces"]["duration_block_pruning"]["version"],
            1
        );
        assert_eq!(
            value["signals"]["traces"]["duration_block_pruning"]["inclusive_bounds"],
            true
        );
        assert_eq!(
            value["signals"]["traces"]["duration_block_pruning"]["legacy_decode_fallback"],
            true
        );
        assert_eq!(
            value["signals"]["traces"]["duration_block_pruning"]["optimize_backfill"],
            true
        );
        assert_eq!(
            value["signals"]["traces"]["attribute_equality"]["version"],
            1
        );
        assert_eq!(
            value["signals"]["traces"]["attribute_equality"]["configuration"],
            "attribute_indexes"
        );
        assert_eq!(
            value["signals"]["traces"]["attribute_equality"]["hidden_input"],
            "attribute_filter"
        );
        assert_eq!(
            value["signals"]["traces"]["attribute_equality"]["max_fields"],
            8
        );
        assert_eq!(
            value["query_surfaces"]["timeless_raw_frame"]["max_work_points"],
            true
        );
        assert_eq!(
            value["query_surfaces"]["timeless_window_batches"]["max_work_points"],
            true
        );
        assert_eq!(
            value["query_surfaces"]["timeless_logs"]["max_work_entries"],
            true
        );
        assert_eq!(
            value["query_surfaces"]["timeless_log_count"]["max_work_entries"],
            true
        );
        assert_eq!(
            value["query_surfaces"]["timeless_log_values"]["max_work_entries"],
            true
        );
        assert_eq!(
            value["query_surfaces"]["timeless_log_query_stats"]["request_local"],
            true
        );
        assert_eq!(
            value["query_surfaces"]["timeless_log_query_stats"]["same_connection"],
            true
        );
        assert_eq!(
            value["query_surfaces"]["timeless_log_query_stats"]["single_use"],
            true
        );
        assert_eq!(
            value["query_surfaces"]["timeless_raw_batches"]["versioned"],
            false
        );
        assert_eq!(
            value["query_surfaces"]["timeless_rollup_batches"]["format"],
            "TRB1"
        );
        assert_eq!(
            value["query_surfaces"]["timeless_aggregate_frame"]["format"],
            "TAF1"
        );
        assert_eq!(
            value["query_surfaces"]["timeless_latest_frame"]["format"],
            "TLF1"
        );
        assert_eq!(
            value["sql_surfaces"]["storage_modules"],
            json!(["timeless_metrics", "timeless_logs", "timeless_traces"])
        );
        assert_eq!(
            value["sql_surfaces"]["query_modules"]
                .as_array()
                .unwrap()
                .len(),
            22
        );
    }
}
