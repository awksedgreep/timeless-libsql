use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use timeless_logs_api::{run, Config};

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(extension_path) = args.next() else {
        eprintln!("usage: timeless-logs-api <libtimeless_ext.so> <database> [listen-address]");
        return ExitCode::from(2);
    };
    let Some(database_path) = args.next() else {
        eprintln!("usage: timeless-logs-api <libtimeless_ext.so> <database> [listen-address]");
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
        eprintln!("usage: timeless-logs-api <libtimeless_ext.so> <database> [listen-address]");
        return ExitCode::from(2);
    }

    let optimize_interval = match interval_from_env("TIMELESS_LOGS_OPTIMIZE_INTERVAL_SECS", 30) {
        Ok(interval) => interval,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    let config = Config {
        extension_path: PathBuf::from(extension_path),
        database_path: PathBuf::from(database_path),
        listen,
        optimize_interval,
        ..Config::default()
    };
    match run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("timeless-logs-api: {error}");
            ExitCode::FAILURE
        }
    }
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
