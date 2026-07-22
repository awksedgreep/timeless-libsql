//! FsStore: the filesystem chunk store — the engine's original
//! persistence layer, moved behind the ChunkStore seam. Individual PCO1
//! chunk files, batched PCB1 files, tmp+rename atomicity, the compaction
//! pending/manifest crash-recovery protocol, the TTL'd whole-file read
//! cache, and disk accounting all live here. On-disk formats are
//! byte-identical to the pre-seam engine: a data_dir written before the
//! refactor recovers unchanged.

use dashmap::DashMap;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use super::{
    ChunkBytes, ChunkLoc, ChunkMeta, ChunkStore, EncodedChunk, StoredChunk, ENC_PCO, ENC_RAW,
};

/// How long a compressed chunk file stays in the read cache.
const FILE_CACHE_TTL: Duration = Duration::from_secs(60);

fn read_exact_at(file: &mut File, offset: u64, len: usize) -> Result<Vec<u8>, String> {
    let mut buf = vec![0u8; len];
    file.seek(SeekFrom::Start(offset))
        .map_err(|e| e.to_string())?;
    file.read_exact(&mut buf).map_err(|e| e.to_string())?;
    Ok(buf)
}

pub struct FsStore {
    data_dir: PathBuf,
    created_dirs: Mutex<HashSet<PathBuf>>,
    batch_counter: AtomicUsize,
    instance_id: u128,
    file_cache: DashMap<PathBuf, (Instant, Arc<Vec<u8>>)>,
}

impl FsStore {
    pub fn new(data_dir: PathBuf) -> Self {
        let instance_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        // Finish any compaction interrupted by a crash BEFORE the engine
        // scans files into its index, so superseded chunks never resurface.
        Self::recover_compaction_manifest(&data_dir);
        FsStore {
            data_dir,
            created_dirs: Mutex::new(HashSet::new()),
            batch_counter: AtomicUsize::new(0),
            instance_id,
            file_cache: DashMap::new(),
        }
    }

    fn created_dirs_lock(&self) -> MutexGuard<'_, HashSet<PathBuf>> {
        self.created_dirs.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn forget_created_dir(&self, dir: &std::path::Path) {
        self.created_dirs_lock().remove(dir);
    }

    fn series_path(data_dir: &std::path::Path) -> PathBuf {
        data_dir.join("series.bin")
    }

    fn manifest_path(data_dir: &std::path::Path) -> PathBuf {
        data_dir.join("compaction.manifest")
    }

    fn ensure_dir(&self, path: &std::path::Path) -> io::Result<()> {
        let dir = path
            .parent()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("path has no parent: {:?}", path),
                )
            })?
            .to_path_buf();
        let mut dirs = self.created_dirs_lock();
        if !dirs.contains(&dir) {
            fs::create_dir_all(&dir)?;
            dirs.insert(dir);
        }
        Ok(())
    }

    fn next_file_id(&self) -> String {
        let seq = self.batch_counter.fetch_add(1, Ordering::Relaxed);
        format!("{}_{:08}", self.instance_id, seq)
    }

    // ── Individual chunk writer (PCO1) ───────────────────────────────

    /// Write a chunk file. With `pending`, the file is left at
    /// `<final>.pending` — invisible to scan() — and the caller renames
    /// it to the final path later (compaction manifest protocol). The
    /// first returned path is always the FINAL path; the second is the
    /// path actually on disk.
    fn write_individual_chunk_at(
        &self,
        cp: &EncodedChunk,
        pending: bool,
    ) -> Result<(PathBuf, PathBuf), String> {
        let series_id_str = cp.series_id.to_string();
        let file_id = self.next_file_id();

        let path = self
            .data_dir
            .join("chunks")
            .join(&series_id_str)
            .join(format!("{}_{}.pco1", cp.min_ts, file_id));

        self.ensure_dir(&path)
            .map_err(|err| format!("failed to create chunk dir {}: {err}", path.display()))?;

        // Store series_id as the partition key string in PCO1
        let pk_bytes = series_id_str.as_bytes();

        let mut out =
            Vec::with_capacity(64 + pk_bytes.len() + cp.ts_bytes.len() + cp.val_bytes.len());
        out.extend_from_slice(b"PCO1");
        // Version byte doubles as payload encoding: 1 = pco, 2 = raw
        out.push(if cp.encoding == ENC_RAW { 2u8 } else { 1u8 });
        out.extend_from_slice(&cp.point_count.to_be_bytes());
        out.extend_from_slice(&cp.min_ts.to_be_bytes());
        out.extend_from_slice(&cp.max_ts.to_be_bytes());
        out.extend_from_slice(&(pk_bytes.len() as u16).to_be_bytes());
        out.extend_from_slice(pk_bytes);
        out.extend_from_slice(&cp.min_val.to_be_bytes());
        out.extend_from_slice(&cp.max_val.to_be_bytes());
        out.extend_from_slice(&cp.sum_val.to_be_bytes());
        out.extend_from_slice(&(cp.ts_bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(&cp.ts_bytes);
        out.extend_from_slice(&(cp.val_bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(&cp.val_bytes);

        let tmp_path = path.with_extension("pco1.tmp");
        fs::File::create(&tmp_path)
            .and_then(|mut file| file.write_all(&out))
            .map_err(|err| format!("failed to write chunk {}: {err}", path.display()))?;

        let written = if pending {
            path.with_extension("pco1.pending")
        } else {
            path.clone()
        };
        fs::rename(&tmp_path, &written)
            .map_err(|err| format!("failed to rename chunk {}: {err}", written.display()))?;

        Ok((path, written))
    }

    // ── Batched chunk writer (PCB1) ──────────────────────────────────

    fn write_batched_chunk(&self, partitions: &[EncodedChunk]) -> Result<Vec<ChunkLoc>, String> {
        let batch_id = self.next_file_id();
        let path = self
            .data_dir
            .join("batches")
            .join(format!("batch_{}.pcb1", batch_id));
        self.ensure_dir(&path)
            .map_err(|err| format!("failed to create batch dir {}: {err}", path.display()))?;

        let n = partitions.len() as u32;
        let header_size = 4 + 1 + 4;
        // Per entry: series_id(8) + point_count(4) + min_ts(8) + max_ts(8) +
        //   min_val(8) + max_val(8) + sum_val(8) + data_offset(8) + data_len(4) = 64
        let entry_size = 64;
        let table_size = n as usize * entry_size;
        let data_start = header_size + table_size;

        let mut data_offsets = Vec::with_capacity(partitions.len());
        let mut offset = data_start;
        for cp in partitions {
            data_offsets.push(offset);
            offset += 4 + cp.ts_bytes.len() + 4 + cp.val_bytes.len();
        }

        let mut out = Vec::with_capacity(offset);

        out.extend_from_slice(b"PCB1");
        // Version byte doubles as payload encoding for ALL partitions in
        // the batch (flushes produce uniform encoding): 1 = pco, 2 = raw
        let batch_encoding = partitions.first().map(|cp| cp.encoding).unwrap_or(ENC_PCO);
        out.push(if batch_encoding == ENC_RAW { 2u8 } else { 1u8 });
        out.extend_from_slice(&n.to_be_bytes());

        for (i, cp) in partitions.iter().enumerate() {
            let data_len = (4 + cp.ts_bytes.len() + 4 + cp.val_bytes.len()) as u32;
            out.extend_from_slice(&cp.series_id.to_be_bytes());
            out.extend_from_slice(&cp.point_count.to_be_bytes());
            out.extend_from_slice(&cp.min_ts.to_be_bytes());
            out.extend_from_slice(&cp.max_ts.to_be_bytes());
            out.extend_from_slice(&cp.min_val.to_be_bytes());
            out.extend_from_slice(&cp.max_val.to_be_bytes());
            out.extend_from_slice(&cp.sum_val.to_be_bytes());
            out.extend_from_slice(&(data_offsets[i] as u64).to_be_bytes());
            out.extend_from_slice(&data_len.to_be_bytes());
        }

        for cp in partitions {
            out.extend_from_slice(&(cp.ts_bytes.len() as u32).to_be_bytes());
            out.extend_from_slice(&cp.ts_bytes);
            out.extend_from_slice(&(cp.val_bytes.len() as u32).to_be_bytes());
            out.extend_from_slice(&cp.val_bytes);
        }

        let tmp_path = path.with_extension("pcb1.tmp");
        fs::File::create(&tmp_path)
            .and_then(|mut file| file.write_all(&out))
            .map_err(|err| format!("failed to write batch {}: {err}", path.display()))?;
        fs::rename(&tmp_path, &path)
            .map_err(|err| format!("failed to rename batch {}: {err}", path.display()))?;

        Ok(partitions
            .iter()
            .enumerate()
            .map(|(i, cp)| {
                let data_len = (4 + cp.ts_bytes.len() + 4 + cp.val_bytes.len()) as u32;
                ChunkLoc::File {
                    path: path.clone(),
                    offset: data_offsets[i] as u64,
                    len: data_len,
                }
            })
            .collect())
    }

    // ── Compaction manifest ──────────────────────────────────────────

    /// Durably record compaction intent: pending->final renames and the
    /// old files to delete. Written via tmp+rename so it is atomic.
    fn write_compaction_manifest(
        &self,
        renames: &[(PathBuf, PathBuf)],
        deletes: &HashSet<PathBuf>,
    ) -> Result<(), String> {
        let mut out = String::new();
        for (pending, final_path) in renames {
            out.push_str(&format!(
                "P\t{}\t{}\n",
                pending.display(),
                final_path.display()
            ));
        }
        for path in deletes {
            out.push_str(&format!("D\t{}\n", path.display()));
        }

        let manifest = Self::manifest_path(&self.data_dir);
        let tmp = manifest.with_extension("manifest.tmp");
        fs::write(&tmp, out).map_err(|e| format!("failed to write manifest: {e}"))?;
        fs::rename(&tmp, &manifest).map_err(|e| format!("failed to commit manifest: {e}"))?;
        Ok(())
    }

    /// Complete an interrupted compaction at startup: finish any pending
    /// renames, delete superseded files, remove the manifest. Called
    /// before the engine scans chunks into its index. If no manifest
    /// exists this is a no-op (stray .pending files from a pre-manifest
    /// crash are swept by scan_dir_recursive instead, leaving the
    /// pre-compaction state).
    fn recover_compaction_manifest(data_dir: &std::path::Path) {
        let manifest = Self::manifest_path(data_dir);
        let Ok(content) = fs::read_to_string(&manifest) else {
            return;
        };

        for line in content.lines() {
            let mut parts = line.split('\t');
            match parts.next() {
                Some("P") => {
                    if let (Some(pending), Some(final_path)) = (parts.next(), parts.next()) {
                        let pending = PathBuf::from(pending);
                        if pending.exists() {
                            let _ = fs::rename(&pending, PathBuf::from(final_path));
                        }
                    }
                }
                Some("D") => {
                    if let Some(path) = parts.next() {
                        let _ = fs::remove_file(path);
                    }
                }
                _ => {}
            }
        }
        let _ = fs::remove_file(&manifest);
    }

    // ── Reading ──────────────────────────────────────────────────────

    /// Whole-file read through the TTL cache (queries touch the same
    /// chunk files repeatedly; batch files carry many chunks).
    fn read_file_cached(&self, path: &PathBuf) -> Result<Arc<Vec<u8>>, String> {
        if let Some(entry) = self.file_cache.get(path) {
            if entry.0.elapsed() < FILE_CACHE_TTL {
                return Ok(Arc::clone(&entry.1));
            }
            drop(entry);
            self.file_cache.remove(path);
        }
        let data: Arc<Vec<u8>> = Arc::new(fs::read(path).map_err(|e| e.to_string())?);
        self.file_cache
            .insert(path.clone(), (Instant::now(), Arc::clone(&data)));
        Ok(data)
    }

    fn parse_partition_data(
        data: &[u8],
        offset: usize,
    ) -> Result<(Range<usize>, Range<usize>), String> {
        if offset + 4 > data.len() {
            return Err(format!("offset {} past file len {}", offset, data.len()));
        }
        let mut pos = offset;
        let ts_size = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + ts_size + 4 > data.len() {
            return Err(format!("ts overrun at {}", offset));
        }
        let ts_range = pos..pos + ts_size;
        pos += ts_size;
        let val_size = u32::from_be_bytes(data[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        if pos + val_size > data.len() {
            return Err(format!("val overrun at {}", offset));
        }
        Ok((ts_range, pos..pos + val_size))
    }

    fn parse_pco1_data(data: &[u8]) -> Result<(Range<usize>, Range<usize>), String> {
        if data.len() < 4 || &data[0..4] != b"PCO1" {
            return Err("invalid PCO1".into());
        }
        let mut pos = 5;
        if pos + 4 + 16 + 2 > data.len() {
            return Err("truncated PCO1 header".into());
        }
        pos += 4;
        pos += 16;
        let pk_len = u16::from_be_bytes(data[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        if pos + pk_len > data.len() {
            return Err("truncated PCO1 partition key".into());
        }
        pos += pk_len;
        if pos + 24 > data.len() {
            return Err("truncated PCO1 metadata".into());
        }
        pos += 24;
        if pos + 4 > data.len() {
            return Err("truncated PCO1 partition data".into());
        }
        Self::parse_partition_data(data, pos)
    }

    // ── Recovery scan ────────────────────────────────────────────────

    fn scan_dir_recursive(dir: &PathBuf, out: &mut Vec<StoredChunk>) {
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                Self::scan_dir_recursive(&path, out);
            } else {
                match path.extension().and_then(|e| e.to_str()) {
                    Some("pco1") => {
                        if let Ok(chunks) = Self::read_pco1_header(&path) {
                            out.extend(chunks);
                        }
                    }
                    Some("pcb1") => {
                        if let Ok(chunks) = Self::read_pcb1_headers(&path) {
                            out.extend(chunks);
                        }
                    }
                    // tmp: interrupted chunk write; pending: compaction
                    // that crashed before its manifest — old chunks are
                    // still intact, so dropping the orphan is correct.
                    Some("tmp") | Some("pending") => {
                        let _ = fs::remove_file(&path);
                    }
                    _ => {}
                }
            }
        }
    }

    fn read_pco1_header(path: &PathBuf) -> Result<Vec<StoredChunk>, String> {
        let mut file = File::open(path).map_err(|e| e.to_string())?;
        let fixed = read_exact_at(&mut file, 0, 31)?;
        if &fixed[0..4] != b"PCO1" {
            return Err("invalid".into());
        }
        let encoding = if fixed[4] == 2 { ENC_RAW } else { ENC_PCO };

        let mut pos = 5;
        let point_count = u32::from_be_bytes(fixed[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let min_ts = i64::from_be_bytes(fixed[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let max_ts = i64::from_be_bytes(fixed[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let pk_len = u16::from_be_bytes(fixed[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        let variable = read_exact_at(&mut file, pos as u64, pk_len + 24)?;
        let pk_str = String::from_utf8_lossy(&variable[0..pk_len]).to_string();
        let mut pos = pk_len;
        let min_val = f64::from_be_bytes(variable[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let max_val = f64::from_be_bytes(variable[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let sum_val = f64::from_be_bytes(variable[pos..pos + 8].try_into().unwrap());

        // pk_str is the series_id as a string
        let series_id = pk_str.parse::<i64>().unwrap_or(0);

        Ok(vec![StoredChunk {
            series_id,
            meta: ChunkMeta {
                min_ts,
                max_ts,
                point_count,
                min_val,
                max_val,
                sum_val,
                loc: ChunkLoc::File {
                    path: path.clone(),
                    offset: 0,
                    len: 0,
                },
                encoding,
            },
        }])
    }

    fn read_pcb1_headers(path: &PathBuf) -> Result<Vec<StoredChunk>, String> {
        let mut file = File::open(path).map_err(|e| e.to_string())?;
        let file_len = file.metadata().map_err(|e| e.to_string())?.len();
        let fixed = read_exact_at(&mut file, 0, 9)?;
        if &fixed[0..4] != b"PCB1" {
            return Err("invalid".into());
        }
        let encoding = if fixed[4] == 2 { ENC_RAW } else { ENC_PCO };

        let n = u32::from_be_bytes(fixed[5..9].try_into().unwrap()) as usize;
        let table_len = n
            .checked_mul(64)
            .ok_or_else(|| "PCB1 table overflow".to_string())?;
        let table_start = 9usize;
        let table_end = table_start
            .checked_add(table_len)
            .ok_or_else(|| "PCB1 table overflow".to_string())?;
        let table = read_exact_at(&mut file, table_start as u64, table_len)?;
        let mut results = Vec::with_capacity(n);
        let mut pos = 0;
        let mut data_entries: Vec<(u64, u32)> = Vec::with_capacity(n);
        for _ in 0..n {
            let series_id = i64::from_be_bytes(table[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let point_count = u32::from_be_bytes(table[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let min_ts = i64::from_be_bytes(table[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let max_ts = i64::from_be_bytes(table[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let min_val = f64::from_be_bytes(table[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let max_val = f64::from_be_bytes(table[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let sum_val = f64::from_be_bytes(table[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let data_offset = u64::from_be_bytes(table[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let data_len = u32::from_be_bytes(table[pos..pos + 4].try_into().unwrap());
            pos += 4;

            data_entries.push((data_offset, data_len));
            results.push(StoredChunk {
                series_id,
                meta: ChunkMeta {
                    min_ts,
                    max_ts,
                    point_count,
                    min_val,
                    max_val,
                    sum_val,
                    loc: ChunkLoc::File {
                        path: path.clone(),
                        offset: data_offset,
                        len: data_len,
                    },
                    encoding,
                },
            });
        }

        for (data_offset, data_len) in data_entries {
            if data_offset < table_end as u64 {
                return Err(format!(
                    "PCB1 data offset {} is within table region (table ends at {})",
                    data_offset, table_end
                ));
            }
            let end = data_offset
                .checked_add(data_len as u64)
                .ok_or_else(|| "PCB1 data entry overflow".to_string())?;
            if end > file_len {
                return Err(format!(
                    "PCB1 data entry overflows file at offset {} (end {} > {})",
                    data_offset, end, file_len
                ));
            }
        }

        Ok(results)
    }

    // ── Disk stats ───────────────────────────────────────────────────

    fn stat_dir_recursive(dir: &PathBuf, total_bytes: &mut u64, file_count: &mut usize) {
        let entries = match fs::read_dir(dir) {
            Ok(e) => e,
            Err(_) => return,
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                Self::stat_dir_recursive(&path, total_bytes, file_count);
            } else if matches!(
                path.extension().and_then(|e| e.to_str()),
                Some("pco1") | Some("pcb1")
            ) {
                if let Ok(meta) = fs::metadata(&path) {
                    *total_bytes += meta.len();
                    *file_count += 1;
                }
            }
        }
    }
}

impl ChunkStore for FsStore {
    fn put_chunks(&self, chunks: &[EncodedChunk]) -> Result<Vec<ChunkLoc>, String> {
        match chunks {
            [] => Ok(Vec::new()),
            // A single chunk gets its own PCO1 file under chunks/<series>/;
            // larger batches pack into one PCB1 file under batches/.
            [one] => {
                let (path, _written) = self.write_individual_chunk_at(one, false)?;
                Ok(vec![ChunkLoc::File {
                    path,
                    offset: 0,
                    len: 0,
                }])
            }
            many => self.write_batched_chunk(many),
        }
    }

    fn replace_chunks(
        &self,
        add: &[EncodedChunk],
        remove: &[ChunkLoc],
        on_committed: &mut dyn FnMut(&[ChunkLoc]),
    ) -> Result<Vec<ChunkLoc>, String> {
        // Phase 1: write every replacement chunk as .pending — invisible
        // to scan() and queries until renamed.
        let mut locs = Vec::with_capacity(add.len());
        let mut renames: Vec<(PathBuf, PathBuf)> = Vec::with_capacity(add.len());
        for cp in add {
            let (path, written) = self.write_individual_chunk_at(cp, true)?;
            renames.push((written, path.clone()));
            locs.push(ChunkLoc::File {
                path,
                offset: 0,
                len: 0,
            });
        }

        let mut deletes: HashSet<PathBuf> = HashSet::new();
        for loc in remove {
            match loc {
                ChunkLoc::File { path, .. } => {
                    deletes.insert(path.clone());
                }
                other => return Err(format!("FsStore cannot remove {:?}", other)),
            }
        }

        // Phase 2: durable intent, then execute. From here, a crash is
        // completed by recovery at next startup.
        self.write_compaction_manifest(&renames, &deletes)?;

        for (pending, final_path) in &renames {
            fs::rename(pending, final_path).map_err(|err| {
                format!("failed to finalize chunk {}: {err}", final_path.display())
            })?;
        }

        // New chunks are live; let the engine swap its index before the
        // old files disappear.
        on_committed(&locs);

        for path in &deletes {
            self.file_cache.remove(path);
            let _ = fs::remove_file(path);
        }
        let _ = fs::remove_file(Self::manifest_path(&self.data_dir));

        Ok(locs)
    }

    fn read_chunk(&self, loc: &ChunkLoc) -> Result<ChunkBytes, String> {
        let ChunkLoc::File { path, offset, .. } = loc else {
            return Err(format!("FsStore cannot read {:?}", loc));
        };
        let data = self.read_file_cached(path)?;
        // offset 0 = individual PCO1 file (payload located via its
        // header); otherwise a slot inside a PCB1 batch file.
        let (ts_range, val_range) = if *offset > 0 {
            Self::parse_partition_data(&data, *offset as usize)?
        } else {
            Self::parse_pco1_data(&data)?
        };
        Ok(ChunkBytes {
            data,
            ts_range,
            val_range,
        })
    }

    fn delete_chunks(&self, locs: &[ChunkLoc]) -> Vec<String> {
        let mut errors = Vec::new();
        for loc in locs {
            let ChunkLoc::File { path, .. } = loc else {
                errors.push(format!("FsStore cannot delete {:?}", loc));
                continue;
            };
            self.file_cache.remove(path);
            if let Err(e) = fs::remove_file(path) {
                errors.push(format!("failed to remove {}: {}", path.display(), e));
            }
            if let Some(dir) = path.parent() {
                match fs::remove_dir(dir) {
                    Ok(()) => self.forget_created_dir(dir),
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {
                        self.forget_created_dir(dir);
                    }
                    Err(_) => {}
                }
            }
        }
        errors
    }

    fn scan(&self) -> Result<Vec<StoredChunk>, String> {
        let mut out = Vec::new();
        for dir_name in &["chunks", "batches"] {
            let dir = self.data_dir.join(dir_name);
            if dir.exists() {
                Self::scan_dir_recursive(&dir, &mut out);
            }
        }
        Ok(out)
    }

    fn save_registry(&self, bytes: &[u8]) -> Result<(), String> {
        let path = Self::series_path(&self.data_dir);
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir).map_err(|e| e.to_string())?;
        }
        let tmp_path = path.with_extension("tmp");
        fs::write(&tmp_path, bytes).map_err(|e| e.to_string())?;
        fs::rename(&tmp_path, &path).map_err(|e| e.to_string())?;
        Ok(())
    }

    fn load_registry(&self) -> Result<Option<Vec<u8>>, String> {
        match fs::read(Self::series_path(&self.data_dir)) {
            Ok(data) => Ok(Some(data)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(err.to_string()),
        }
    }

    fn storage_stats(&self) -> (u64, usize) {
        // Walk the chunk dirs summing file sizes. series.bin and the
        // manifest are bookkeeping, not chunk storage — excluded, same
        // as the pre-seam accounting (unique files in the index).
        let mut total_bytes = 0u64;
        let mut file_count = 0usize;
        for dir_name in &["chunks", "batches"] {
            let dir = self.data_dir.join(dir_name);
            if dir.exists() {
                Self::stat_dir_recursive(&dir, &mut total_bytes, &mut file_count);
            }
        }
        (total_bytes, file_count)
    }

    /// Drop expired file-cache entries. The read path only evicts entries
    /// it happens to touch after expiry, so a file read once and never
    /// again would stay resident forever without this periodic sweep.
    fn sweep_cache(&self) {
        self.file_cache
            .retain(|_, (cached_at, _)| cached_at.elapsed() < FILE_CACHE_TTL);
    }
}
