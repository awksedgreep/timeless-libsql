use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use timeless_api_common::{server_build_identity, AuthConfig};
use timeless_traces_api::{run, Config, TracesQueryLimits};

const USAGE: &str = "usage: timeless-traces-api <libtimeless_ext.so> <database> [listen-address]";

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
        println!("{}", server_build_identity("traces"));
        return ExitCode::SUCCESS;
    }
    let Some(database_path) = args.next() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let listen: SocketAddr = match args
        .next()
        .unwrap_or_else(|| "127.0.0.1:19449".to_owned())
        .parse()
    {
        Ok(address) => address,
        Err(error) => return usage_error(format!("invalid listen address: {error}")),
    };
    if args.next().is_some() {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    }

    let auth = match AuthConfig::from_env("traces") {
        Ok(auth) => auth,
        Err(error) => return usage_error(error),
    };
    if auth.is_open() {
        eprintln!(
            "WARNING: timeless-traces-api starting with authentication DISABLED: \
             ingest, query, and admin routes are open to whoever can reach the listener. \
             Set TIMELESS_AUTH_MODE=required with TIMELESS_AUTH_POLICY_FILE (and \
             optionally TIMELESS_ADMIN_KEY) to lock it down."
        );
    }

    let defaults = Config::default();
    let reader_connections = match positive_usize_from_env(
        "TIMELESS_TRACES_READER_CONNECTIONS",
        defaults.reader_connections,
    ) {
        Ok(value) => value,
        Err(error) => return usage_error(error),
    };
    let command_queue_batches = match positive_usize_from_env(
        "TIMELESS_TRACES_COMMAND_QUEUE_BATCHES",
        defaults.command_queue_batches,
    ) {
        Ok(value) => value,
        Err(error) => return usage_error(error),
    };
    let queue_bytes = match positive_usize_from_env("TIMELESS_TRACES_QUEUE_BYTES", defaults.queue_bytes)
    {
        Ok(value) => value,
        Err(error) => return usage_error(error),
    };
    let enforce_retention = std::env::var_os("TIMELESS_TRACES_RETENTION_SECS").is_some();
    let retention =
        match optional_duration_from_env("TIMELESS_TRACES_RETENTION_SECS", defaults.retention) {
            Ok(value) => value,
            Err(error) => return usage_error(error),
        };
    let flush_interval = match duration_from_env(
        "TIMELESS_TRACES_FLUSH_INTERVAL_SECS",
        defaults.flush_interval,
    ) {
        Ok(value) => value,
        Err(error) => return usage_error(error),
    };
    let optimize_interval = match duration_from_env(
        "TIMELESS_TRACES_OPTIMIZE_INTERVAL_SECS",
        defaults.optimize_interval,
    ) {
        Ok(value) => value,
        Err(error) => return usage_error(error),
    };

    let config = Config {
        extension_path: PathBuf::from(extension_path),
        database_path: PathBuf::from(database_path),
        listen,
        reader_connections,
        command_queue_batches,
        queue_bytes,
        retention,
        enforce_retention,
        flush_interval,
        optimize_interval,
        query_limits: TracesQueryLimits::default(),
        auth,
    };
    match run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("timeless-traces-api: {error}");
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
    let parsed = value
        .parse::<usize>()
        .map_err(|error| format!("invalid {name}={value:?}: {error}"))?;
    if parsed == 0 {
        return Err(format!("{name} must be positive"));
    }
    Ok(parsed)
}

fn duration_from_env(name: &str, default: Duration) -> Result<Duration, String> {
    match optional_duration_from_env(name, Some(default))? {
        Some(duration) => Ok(duration),
        None => Err(format!("{name} must be positive")),
    }
}

fn optional_duration_from_env(
    name: &str,
    default: Option<Duration>,
) -> Result<Option<Duration>, String> {
    let value = match std::env::var(name) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(default),
        Err(error) => return Err(format!("read {name}: {error}")),
    };
    let seconds = value
        .parse::<u64>()
        .map_err(|error| format!("invalid {name}={value:?}: {error}"))?;
    Ok((seconds != 0).then(|| Duration::from_secs(seconds)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_retention_disables_the_vtab_argument_but_not_maintenance() {
        assert_eq!(
            optional_duration_from_env_value("RETENTION", "0").unwrap(),
            None
        );
        assert_eq!(
            optional_duration_from_env_value("RETENTION", "7").unwrap(),
            Some(Duration::from_secs(7))
        );
        assert_eq!(
            duration_from_env_value("FLUSH", "0").unwrap_err(),
            "FLUSH must be positive"
        );
    }

    fn optional_duration_from_env_value(
        name: &str,
        value: &str,
    ) -> Result<Option<Duration>, String> {
        let seconds = value
            .parse::<u64>()
            .map_err(|error| format!("invalid {name}={value:?}: {error}"))?;
        Ok((seconds != 0).then(|| Duration::from_secs(seconds)))
    }

    fn duration_from_env_value(name: &str, value: &str) -> Result<Duration, String> {
        optional_duration_from_env_value(name, value)?
            .ok_or_else(|| format!("{name} must be positive"))
    }
}
