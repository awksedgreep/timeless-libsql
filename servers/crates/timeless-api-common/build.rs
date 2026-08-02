use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=TIMELESS_BUILD_COMMIT");
    let commit = std::env::var("TIMELESS_BUILD_COMMIT")
        .ok()
        .filter(|value| !value.is_empty())
        .or_else(git_commit)
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=TIMELESS_BUILD_COMMIT_RESOLVED={commit}");
    println!(
        "cargo:rustc-env=TIMELESS_BUILD_TARGET={}",
        std::env::var("TARGET").unwrap_or_else(|_| "unknown".to_owned())
    );
    println!(
        "cargo:rustc-env=TIMELESS_BUILD_PROFILE={}",
        std::env::var("PROFILE").unwrap_or_else(|_| "unknown".to_owned())
    );
}

fn git_commit() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
