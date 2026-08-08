use std::fs::{self, File};
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use flate2::{Compression, GzBuilder};
use tar::{Builder, EntryType, Header};

pub(crate) fn deterministic_tar(source: &Path, destination: &Path, epoch: u64) -> Result<()> {
    let prefix = source
        .file_name()
        .context("bundle directory has no file name")?;
    let mut paths = vec![source.to_path_buf()];
    collect_sorted(source, &mut paths)?;

    let output = File::create(destination)
        .with_context(|| format!("create archive {}", destination.display()))?;
    let gzip = GzBuilder::new().mtime(0).write(output, Compression::best());
    let mut archive = Builder::new(gzip);
    for path in paths {
        let relative = path.strip_prefix(source)?;
        let archive_path = Path::new(prefix).join(relative);
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.file_type().is_symlink() {
            bail!("release archive cannot contain symlink {}", path.display());
        }
        let mut header = Header::new_ustar();
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(epoch);
        header.set_username("root")?;
        header.set_groupname("root")?;
        if metadata.is_dir() {
            header.set_entry_type(EntryType::Directory);
            header.set_mode(0o755);
            header.set_size(0);
            header.set_cksum();
            archive.append_data(&mut header, archive_path, io::empty())?;
        } else if metadata.is_file() {
            header.set_entry_type(EntryType::Regular);
            header.set_mode(if metadata.permissions().mode() & 0o100 != 0 {
                0o755
            } else {
                0o644
            });
            header.set_size(metadata.len());
            header.set_cksum();
            archive.append_data(&mut header, archive_path, File::open(&path)?)?;
        } else {
            bail!("unsupported release payload type: {}", path.display());
        }
    }
    let gzip = archive.into_inner()?;
    gzip.finish()?;
    Ok(())
}

fn collect_sorted(directory: &Path, output: &mut Vec<PathBuf>) -> Result<()> {
    let mut children = fs::read_dir(directory)?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<io::Result<Vec<_>>>()?;
    children.sort();
    for child in children {
        output.push(child.clone());
        if child.is_dir() {
            collect_sorted(&child, output)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn archive_is_byte_reproducible_and_normalized() {
        let temporary = tempfile::Builder::new()
            .prefix("archive-test-")
            .tempdir_in(Path::new(env!("CARGO_MANIFEST_DIR")).join("target"))
            .unwrap();
        let source = temporary.path().join("bundle");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("payload"), b"timeless\n").unwrap();
        fs::set_permissions(source.join("payload"), fs::Permissions::from_mode(0o600)).unwrap();
        let first = temporary.path().join("first.tar.gz");
        let second = temporary.path().join("second.tar.gz");

        deterministic_tar(&source, &first, 1_700_000_000).unwrap();
        deterministic_tar(&source, &second, 1_700_000_000).unwrap();
        assert_eq!(fs::read(&first).unwrap(), fs::read(&second).unwrap());

        let decoder = flate2::read::GzDecoder::new(File::open(first).unwrap());
        let mut archive = tar::Archive::new(decoder);
        let mut entries = archive.entries().unwrap();
        let root = entries.next().unwrap().unwrap();
        assert_eq!(root.path().unwrap(), Path::new("bundle"));
        assert_eq!(root.header().mode().unwrap(), 0o755);
        let mut payload = entries.next().unwrap().unwrap();
        assert_eq!(payload.path().unwrap(), Path::new("bundle/payload"));
        assert_eq!(payload.header().mode().unwrap(), 0o644);
        let mut contents = String::new();
        payload.read_to_string(&mut contents).unwrap();
        assert_eq!(contents, "timeless\n");
    }
}
