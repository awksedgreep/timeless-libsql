use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use timeless_api_common::{server_build_identity, AuthConfig};
use timeless_logs_api::{run, Config};

const USAGE: &str = "usage: timeless-logs-api <libtimeless_ext.so> <database> [listen-address]";

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
        println!("{}", server_build_identity("logs"));
        return ExitCode::SUCCESS;
    }
    let Some(database_path) = args.next() else {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    };
    let listen: SocketAddr = match args
        .next()
        .unwrap_or_else(|| "127.0.0.1:19429".to_string())
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

    let auth = match AuthConfig::required_from_env("logs") {
        Ok(auth) => auth,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };

    let defaults = Config::default();
    let flush_interval = match interval_from_env(
        "TIMELESS_LOGS_FLUSH_INTERVAL_SECS",
        defaults.flush_interval.as_secs(),
    ) {
        Ok(interval) => interval,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    let optimize_interval = match interval_from_env(
        "TIMELESS_LOGS_OPTIMIZE_INTERVAL_SECS",
        defaults.optimize_interval.as_secs(),
    ) {
        Ok(interval) => interval,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    let reader_connections = match positive_usize_from_env(
        "TIMELESS_LOGS_READER_CONNECTIONS",
        defaults.reader_connections,
    ) {
        Ok(connections) => connections,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    let command_queue_batches = match positive_usize_from_env(
        "TIMELESS_LOGS_COMMAND_QUEUE_BATCHES",
        defaults.command_queue_batches,
    ) {
        Ok(batches) => batches,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    let config = Config {
        extension_path: PathBuf::from(extension_path),
        database_path: PathBuf::from(database_path),
        listen,
        reader_connections,
        command_queue_batches,
        flush_interval,
        optimize_interval,
        auth,
        ..defaults
    };
    match run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("timeless-logs-api: {error}");
            ExitCode::FAILURE
        }
    }
}

fn positive_usize_from_env(name: &str, default: usize) -> Result<usize, String> {
    let value = match std::env::var(name) {
        Ok(value) => return parse_positive_usize(name, &value),
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => return Err(format!("read {name}: {error}")),
    };
    if value == 0 {
        return Err(format!("{name} must be positive"));
    }
    Ok(value)
}

fn parse_positive_usize(name: &str, value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|error| format!("invalid {name}={value:?}: {error}"))?;
    if parsed == 0 {
        return Err(format!("{name} must be positive"));
    }
    Ok(parsed)
}

fn interval_from_env(name: &str, default_seconds: u64) -> Result<Duration, String> {
    let seconds = match std::env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .map_err(|error| format!("invalid {name}={value:?}: {error}"))?,
        Err(std::env::VarError::NotPresent) => default_seconds,
        Err(error) => return Err(format!("read {name}: {error}")),
    };
    if seconds == 0 {
        return Err(format!("{name} must be positive"));
    }
    Ok(Duration::from_secs(seconds))
}

#[cfg(test)]
mod tests {
    use super::{interval_from_env, parse_positive_usize};
    use std::time::Duration;

    #[test]
    fn reader_override_requires_a_positive_integer() {
        assert_eq!(parse_positive_usize("READERS", "4").unwrap(), 4);
        assert_eq!(
            parse_positive_usize("READERS", "0").unwrap_err(),
            "READERS must be positive"
        );
        assert!(parse_positive_usize("READERS", "many")
            .unwrap_err()
            .starts_with("invalid READERS=\"many\":"));
    }

    #[test]
    fn zero_maintenance_interval_is_rejected() {
        const NAME: &str = "TIMELESS_LOGS_TEST_INTERVAL_SECS";
        std::env::set_var(NAME, "0");
        assert_eq!(
            interval_from_env(NAME, 30).unwrap_err(),
            format!("{NAME} must be positive")
        );
        std::env::remove_var(NAME);
        assert_eq!(
            interval_from_env(NAME, 30).unwrap(),
            Duration::from_secs(30)
        );
    }
}
