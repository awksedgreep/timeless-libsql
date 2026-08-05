//! Per-connection handoff for one completed log query report.
//!
//! SQLite module auxiliary data is owned by one host connection. Sharing this
//! state between `timeless_logs` and `timeless_log_query_stats` therefore gives
//! the report the correct scope without a process-global map, raw connection
//! pointer identity, or before/after deltas from cumulative engine counters.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use timeless_core::LogQueryExecutionReport;

#[derive(Default)]
pub struct LogQueryReportState {
    reports: Mutex<HashMap<(String, String), LogQueryExecutionReport>>,
}

impl LogQueryReportState {
    fn lock(&self) -> MutexGuard<'_, HashMap<(String, String), LogQueryExecutionReport>> {
        self.reports
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    pub(crate) fn clear(&self, database: &str, table: &str) {
        self.lock().remove(&(database.to_owned(), table.to_owned()));
    }

    pub(crate) fn publish(&self, database: &str, table: &str, report: LogQueryExecutionReport) {
        self.lock()
            .insert((database.to_owned(), table.to_owned()), report);
    }

    /// Consume rather than clone the report. A second read must not silently
    /// reuse stale work after another statement, failure, or transaction edge.
    pub(crate) fn take(&self, database: &str, table: &str) -> Option<LogQueryExecutionReport> {
        self.lock().remove(&(database.to_owned(), table.to_owned()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reports_are_table_scoped_and_single_use() {
        let state = LogQueryReportState::default();
        let first = LogQueryExecutionReport {
            matched_entries: 7,
            ..LogQueryExecutionReport::default()
        };
        let second = LogQueryExecutionReport {
            matched_entries: 11,
            ..LogQueryExecutionReport::default()
        };
        state.publish("main", "a", first);
        state.publish("main", "b", second);

        assert_eq!(state.take("main", "a"), Some(first));
        assert_eq!(state.take("main", "a"), None);
        assert_eq!(state.take("main", "b"), Some(second));
    }

    #[test]
    fn clearing_one_table_never_discards_another() {
        let state = LogQueryReportState::default();
        state.publish("main", "a", LogQueryExecutionReport::default());
        state.publish("audit", "a", LogQueryExecutionReport::default());

        state.clear("main", "a");
        assert_eq!(state.take("main", "a"), None);
        assert!(state.take("audit", "a").is_some());
    }
}
