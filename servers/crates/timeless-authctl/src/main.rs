//! CLI over the authctl library. Argument handling is hand-rolled to match
//! the servers' zero-framework style; every command prints exactly what the
//! quickstart in docs/SERVER_API_REFERENCE.md shows.

use std::path::PathBuf;
use std::process::ExitCode;

fn usage() -> ExitCode {
    eprintln!(
        "usage:
  timeless-authctl keygen --out <dir>
  timeless-authctl policy init --signal <metrics|logs|traces> --key <base64url-public-key> \\
      --out <path> [--subject <name>] [--tenant <tenant>]
  timeless-authctl policy add-subject --policy <path> --subject <name> --scopes <a,b,c>
  timeless-authctl token mint --key <private-key-path> --policy <path> --subject <name> \\
      --signal <metrics|logs|traces> [--ttl <30s|15m|1h|2d>]
  timeless-authctl token inspect <token>"
    );
    ExitCode::from(2)
}

fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|arg| arg == name)
        .and_then(|idx| args.get(idx + 1))
        .cloned()
}

fn fail(error: String) -> ExitCode {
    eprintln!("timeless-authctl: {error}");
    ExitCode::from(1)
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        // The release tool identity-checks every bundled binary with
        // --version and expects the same JSON shape the servers emit.
        Some("--version") if args.len() == 1 => {
            println!(
                "{}",
                serde_json::json!({
                    "name": "timeless-authctl",
                    "version": env!("CARGO_PKG_VERSION"),
                    "commit": env!("TIMELESS_BUILD_COMMIT_RESOLVED"),
                    "target": env!("TIMELESS_BUILD_TARGET"),
                    "profile": env!("TIMELESS_BUILD_PROFILE")
                })
            );
            ExitCode::SUCCESS
        }
        Some("keygen") => {
            let Some(out) = flag(&args, "--out") else {
                return usage();
            };
            match timeless_authctl::keygen(&PathBuf::from(out)) {
                Ok(keypair) => {
                    println!("kid: {}", keypair.kid);
                    println!("public_key: {}", keypair.public_key);
                    println!(
                        "private key written to {} (mode 0600)",
                        timeless_authctl::PRIVATE_KEY_FILE
                    );
                    ExitCode::SUCCESS
                }
                Err(error) => fail(error),
            }
        }
        Some("policy") => match args.get(1).map(String::as_str) {
            Some("init") => {
                let (Some(signal), Some(key), Some(out)) = (
                    flag(&args, "--signal"),
                    flag(&args, "--key"),
                    flag(&args, "--out"),
                ) else {
                    return usage();
                };
                let subject = flag(&args, "--subject").unwrap_or_else(|| "default".into());
                let tenant = flag(&args, "--tenant").unwrap_or_else(|| "default".into());
                match timeless_authctl::policy_init(
                    &signal,
                    &key,
                    &subject,
                    &tenant,
                    &PathBuf::from(&out),
                ) {
                    Ok(_) => {
                        println!("policy written to {out}");
                        ExitCode::SUCCESS
                    }
                    Err(error) => fail(error),
                }
            }
            Some("add-subject") => {
                let (Some(policy), Some(subject), Some(scopes)) = (
                    flag(&args, "--policy"),
                    flag(&args, "--subject"),
                    flag(&args, "--scopes"),
                ) else {
                    return usage();
                };
                let scopes: Vec<String> = scopes.split(',').map(str::to_owned).collect();
                match timeless_authctl::policy_add_subject(
                    &PathBuf::from(policy),
                    &subject,
                    &scopes,
                ) {
                    Ok(false) => {
                        println!("added subject {subject:?} with {} scope(s)", scopes.len());
                        ExitCode::SUCCESS
                    }
                    Ok(true) => {
                        println!(
                            "replaced subject {subject:?} with {} scope(s)",
                            scopes.len()
                        );
                        ExitCode::SUCCESS
                    }
                    Err(error) => fail(error),
                }
            }
            _ => usage(),
        },
        Some("token") => match args.get(1).map(String::as_str) {
            Some("mint") => {
                let (Some(key), Some(policy), Some(subject), Some(signal)) = (
                    flag(&args, "--key"),
                    flag(&args, "--policy"),
                    flag(&args, "--subject"),
                    flag(&args, "--signal"),
                ) else {
                    return usage();
                };
                let ttl = match timeless_authctl::parse_ttl(
                    &flag(&args, "--ttl").unwrap_or_else(|| "1h".into()),
                ) {
                    Ok(ttl) => ttl,
                    Err(error) => return fail(error),
                };
                match timeless_authctl::mint(
                    &PathBuf::from(key),
                    &PathBuf::from(policy),
                    &subject,
                    &signal,
                    ttl,
                ) {
                    Ok(token) => {
                        println!("{token}");
                        ExitCode::SUCCESS
                    }
                    Err(error) => fail(error),
                }
            }
            Some("inspect") => {
                let Some(token) = args.get(2) else {
                    return usage();
                };
                match timeless_authctl::inspect(token) {
                    Ok((header, claims)) => {
                        println!("{}", serde_json::to_string_pretty(&header).unwrap());
                        println!("{}", serde_json::to_string_pretty(&claims).unwrap());
                        ExitCode::SUCCESS
                    }
                    Err(error) => fail(error),
                }
            }
            _ => usage(),
        },
        _ => usage(),
    }
}
