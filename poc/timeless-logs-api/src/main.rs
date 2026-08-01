use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

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

    let config = Config {
        extension_path: PathBuf::from(extension_path),
        database_path: PathBuf::from(database_path),
        listen,
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
