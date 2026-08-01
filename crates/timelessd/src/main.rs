use std::process::ExitCode;

use timelessd::Config;

#[tokio::main]
async fn main() -> ExitCode {
    let config = match Config::from_env_args() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("timelessd: {error}");
            return ExitCode::from(2);
        }
    };
    match timelessd::run(config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("timelessd: {error}");
            ExitCode::FAILURE
        }
    }
}
