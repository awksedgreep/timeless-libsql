use std::env;
use std::fs;
use std::path::PathBuf;

pub(crate) struct Scratch {
    path: PathBuf,
    temporary: Option<tempfile::TempDir>,
}

impl Scratch {
    pub(crate) fn from_args(program: &str, prefix: &str) -> (String, Self) {
        let mut arguments = env::args().skip(1);
        let extension = arguments.next().unwrap_or_else(|| usage(program));
        let mut keep_directory = None;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--keep-dir" => {
                    let path = arguments.next().unwrap_or_else(|| usage(program));
                    if keep_directory.replace(PathBuf::from(path)).is_some() {
                        usage(program);
                    }
                }
                _ => usage(program),
            }
        }

        let scratch = if let Some(path) = keep_directory {
            if path.exists() {
                eprintln!(
                    "benchmark keep directory already exists: {}",
                    path.display()
                );
                std::process::exit(2);
            }
            fs::create_dir_all(&path).unwrap_or_else(|error| {
                panic!(
                    "create benchmark keep directory {}: {error}",
                    path.display()
                )
            });
            Self {
                path,
                temporary: None,
            }
        } else {
            Self::temporary(prefix)
        };
        (extension, scratch)
    }

    fn temporary(prefix: &str) -> Self {
        let temporary = tempfile::Builder::new()
            .prefix(prefix)
            .tempdir()
            .expect("create benchmark scratch directory");
        Self {
            path: temporary.path().to_path_buf(),
            temporary: Some(temporary),
        }
    }

    pub(crate) fn database(&self, name: &str) -> String {
        self.path.join(name).to_string_lossy().into_owned()
    }

    pub(crate) fn finish_message(&self) -> String {
        if self.temporary.is_some() {
            "temporary benchmark databases removed".to_owned()
        } else {
            format!("benchmark databases kept at {}", self.path.display())
        }
    }
}

fn usage(program: &str) -> ! {
    eprintln!("usage: {program} <path-to-libtimeless_ext.so> [--keep-dir NEW_DIRECTORY]");
    std::process::exit(2);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporary_scratch_is_removed_on_drop() {
        let scratch = Scratch::temporary("timeless-bench-cleanup-test-");
        let path = scratch.path.clone();
        fs::write(path.join("large.db"), b"temporary").unwrap();
        drop(scratch);
        assert!(!path.exists());
    }
}
