use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use timeless_api_common::{server_build_identity, AuthConfig};
use timeless_metrics_api::{run, Config, PromQueryLimits};

const USAGE: &str = "usage: timeless-metrics-api <libtimeless_ext.so> <database> [listen-address]";

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(extension_path) = args.next() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    if extension_path == "--version" {
        if args.next().is_some() {
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
        println!("{}", server_build_identity("metrics"));
        return ExitCode::SUCCESS;
    }
    let Some(database_path) = args.next() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let listen: SocketAddr = match args
        .next()
        .unwrap_or_else(|| "127.0.0.1:19439".to_string())
        .parse()
    {
        Ok(listen) => listen,
        Err(error) => {
            eprintln!("invalid listen address: {error}");
            return ExitCode::from(2);
        }
    };
    if args.next().is_some() {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    }

    let auth = match AuthConfig::from_env("metrics") {
        Ok(auth) => auth,
        Err(error) => return usage_error(error),
    };

    let defaults = Config::default();
    let reader_connections = match positive_usize_from_env(
        "TIMELESS_METRICS_READER_CONNECTIONS",
        defaults.reader_connections,
    ) {
        Ok(value) => value,
        Err(error) => return usage_error(error),
    };
    let command_queue_batches = match positive_usize_from_env(
        "TIMELESS_METRICS_COMMAND_QUEUE_BATCHES",
        defaults.command_queue_batches,
    ) {
        Ok(value) => value,
        Err(error) => return usage_error(error),
    };
    let flush_interval = match interval_from_env(
        "TIMELESS_METRICS_FLUSH_INTERVAL_SECS",
        defaults.flush_interval,
    ) {
        Ok(value) => value,
        Err(error) => return usage_error(error),
    };
    let compact_interval = match interval_from_env(
        "TIMELESS_METRICS_COMPACT_INTERVAL_SECS",
        defaults.compact_interval,
    ) {
        Ok(value) => value,
        Err(error) => return usage_error(error),
    };
    let retention_interval = match interval_from_env(
        "TIMELESS_METRICS_RETENTION_INTERVAL_SECS",
        defaults.retention_interval,
    ) {
        Ok(value) => value,
        Err(error) => return usage_error(error),
    };
    let prom_query_limits = PromQueryLimits {
        max_points_per_series: match positive_usize_from_env(
            "TIMELESS_METRICS_PROMQL_MAX_POINTS_PER_SERIES",
            defaults.prom_query_limits.max_points_per_series,
        ) {
            Ok(value) => value,
            Err(error) => return usage_error(error),
        },
        max_result_points: match positive_usize_from_env(
            "TIMELESS_METRICS_PROMQL_MAX_RESULT_POINTS",
            defaults.prom_query_limits.max_result_points,
        ) {
            Ok(value) => value,
            Err(error) => return usage_error(error),
        },
        max_work_points: match positive_usize_from_env(
            "TIMELESS_METRICS_PROMQL_MAX_WORK_POINTS",
            defaults.prom_query_limits.max_work_points,
        ) {
            Ok(value) => value,
            Err(error) => return usage_error(error),
        },
        max_response_bytes: match positive_usize_from_env(
            "TIMELESS_METRICS_PROMQL_MAX_RESPONSE_BYTES",
            defaults.prom_query_limits.max_response_bytes,
        ) {
            Ok(value) => value,
            Err(error) => return usage_error(error),
        },
        default_subquery_step: match duration_millis_from_env(
            "TIMELESS_METRICS_PROMQL_DEFAULT_SUBQUERY_STEP_MS",
            defaults.prom_query_limits.default_subquery_step,
        ) {
            Ok(value) => value,
            Err(error) => return usage_error(error),
        },
        deadline: match duration_millis_from_env(
            "TIMELESS_METRICS_PROMQL_DEADLINE_MS",
            defaults.prom_query_limits.deadline,
        ) {
            Ok(value) => value,
            Err(error) => return usage_error(error),
        },
    };

    let config = Config {
        extension_path: PathBuf::from(extension_path),
        database_path: PathBuf::from(database_path),
        listen,
        reader_connections,
        command_queue_batches,
        flush_interval,
        compact_interval,
        retention_interval,
        prom_query_limits,
        auth,
        ..defaults
    };

    match run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("timeless-metrics-api: {error}");
            ExitCode::FAILURE
        }
    }
}

fn usage_error(error: String) -> ExitCode {
    eprintln!("{error}");
    ExitCode::from(2)
}

fn positive_usize_from_env(name: &str, default: usize) -> Result<usize, String> {
    let value = match std::env::var(name) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(default),
        Err(error) => return Err(format!("read {name}: {error}")),
    };
    let value = value
        .parse::<usize>()
        .map_err(|error| format!("invalid {name}={value:?}: {error}"))?;
    if value == 0 {
        return Err(format!("{name} must be positive"));
    }
    Ok(value)
}

fn interval_from_env(name: &str, default: Duration) -> Result<Duration, String> {
    let value = match std::env::var(name) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(default),
        Err(error) => return Err(format!("read {name}: {error}")),
    };
    let seconds = value
        .parse::<u64>()
        .map_err(|error| format!("invalid {name}={value:?}: {error}"))?;
    if seconds == 0 {
        return Err(format!("{name} must be positive"));
    }
    Ok(Duration::from_secs(seconds))
}

fn duration_millis_from_env(name: &str, default: Duration) -> Result<Duration, String> {
    let value = match std::env::var(name) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(default),
        Err(error) => return Err(format!("read {name}: {error}")),
    };
    let milliseconds = value
        .parse::<u64>()
        .map_err(|error| format!("invalid {name}={value:?}: {error}"))?;
    if milliseconds == 0 {
        return Err(format!("{name} must be positive"));
    }
    Ok(Duration::from_millis(milliseconds))
}

#[cfg(test)]
mod tests {
    #[test]
    fn environment_overrides_require_positive_values() {
        assert_eq!(positive_usize_from_env_value("READERS", "4").unwrap(), 4);
        assert_eq!(
            positive_usize_from_env_value("READERS", "0").unwrap_err(),
            "READERS must be positive"
        );
        assert!(positive_usize_from_env_value("READERS", "many")
            .unwrap_err()
            .starts_with("invalid READERS=\"many\":"));
    }

    fn positive_usize_from_env_value(name: &str, value: &str) -> Result<usize, String> {
        let parsed = value
            .parse::<usize>()
            .map_err(|error| format!("invalid {name}={value:?}: {error}"))?;
        if parsed == 0 {
            return Err(format!("{name} must be positive"));
        }
        Ok(parsed)
    }
}
