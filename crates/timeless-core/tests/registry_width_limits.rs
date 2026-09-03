//! Width-limit regression tests (issue #40).
//!
//! The series-registry wire format prefixes names/labels with u16
//! lengths. A bare `as u16` cast would silently wrap past 64 KiB and
//! persist a corrupted registry that decodes as different, shorter
//! names. These tests pin the loud behavior end to end: resolving an
//! over-wide series is fine in memory, but `flush_all` (which persists
//! the registry) must return a clean `Err` and must not hand the store
//! wrapped bytes.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use timeless_core::{
    ChunkBytes, ChunkLoc, ChunkStore, EncodedChunk, Engine, Labels, ResolvedSeries, StoredChunk,
    StoredSeries,
};

struct Probe {
    saved: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl ChunkStore for Probe {
    fn put_chunks(&self, chunks: &[EncodedChunk]) -> Result<Vec<ChunkLoc>, String> {
        Ok(chunks
            .iter()
            .enumerate()
            .map(|(i, _)| ChunkLoc::Row {
                rowid: i as i64 + 1,
            })
            .collect())
    }

    fn replace_chunks(
        &self,
        _add: &[EncodedChunk],
        _remove: &[ChunkLoc],
        _on_committed: &mut dyn FnMut(&[ChunkLoc]),
    ) -> Result<Vec<ChunkLoc>, String> {
        Err("not used by width-limit tests".into())
    }

    fn read_chunk(&self, _loc: &ChunkLoc) -> Result<ChunkBytes, String> {
        Err("not used by width-limit tests".into())
    }

    fn delete_chunks(&self, _locs: &[ChunkLoc]) -> Vec<String> {
        Vec::new()
    }

    fn scan(&self) -> Result<Vec<StoredChunk>, String> {
        Ok(Vec::new())
    }

    fn save_registry(&self, bytes: &[u8]) -> Result<(), String> {
        self.saved.lock().unwrap().push(bytes.to_vec());
        Ok(())
    }

    fn load_registry(&self) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }

    fn has_authoritative_series(&self) -> bool {
        // Non-authoritative: the engine owns the registry and must
        // serialize it through `save_registry` on flush.
        false
    }

    fn load_series(&self) -> Result<Vec<StoredSeries>, String> {
        Ok(Vec::new())
    }

    fn resolve_series(
        &self,
        _name: &str,
        _labels: &[(String, String)],
    ) -> Result<ResolvedSeries, String> {
        Err("not used by width-limit tests".into())
    }

    fn storage_stats(&self) -> (u64, usize) {
        (0, 0)
    }

    fn sweep_cache(&self) {}
}

fn build() -> (Engine, Arc<Mutex<Vec<Vec<u8>>>>) {
    let saved = Arc::new(Mutex::new(Vec::<Vec<u8>>::new()));
    let eng = Engine::with_store(
        Box::new(Probe {
            saved: Arc::clone(&saved),
        }),
        4096,
        0,
        8,
        64 * 1024 * 1024,
        false,
    )
    .unwrap();
    (eng, saved)
}

fn labels(pairs: &[(&str, &str)]) -> Labels {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<BTreeMap<_, _>>()
}

#[test]
fn over_wide_label_value_fails_flush_without_persisting() {
    let (eng, saved) = build();
    // 70_000 bytes > u16::MAX: realistic for an unlucky label value
    // (URLs, query strings, baked-in trace ids) and the exact shape
    // that used to wrap the u16 prefix silently.
    let big = "v".repeat(70_000);
    let entries = vec![("cpu_usage".to_string(), labels(&[("host", big.as_str())]))];
    eng.resolve_series_batch(&entries).unwrap();

    let err = eng
        .flush_all()
        .expect_err("over-wide label must fail persist");
    assert!(
        err.contains("u16::MAX"),
        "error must name the width limit, got: {err}"
    );
    assert!(
        saved.lock().unwrap().is_empty(),
        "no wrapped registry bytes may reach the store"
    );
}

#[test]
fn over_wide_metric_name_fails_flush_without_persisting() {
    let (eng, saved) = build();
    let big_name = "m".repeat(70_000);
    let entries = vec![(big_name, labels(&[("host", "web1")]))];
    eng.resolve_series_batch(&entries).unwrap();

    let err = eng
        .flush_all()
        .expect_err("over-wide metric name must fail persist");
    assert!(
        err.contains("u16::MAX"),
        "error must name the width limit, got: {err}"
    );
    assert!(
        saved.lock().unwrap().is_empty(),
        "no wrapped registry bytes may reach the store"
    );
}

#[test]
fn boundary_lengths_still_persist() {
    let (eng, saved) = build();
    // Exactly u16::MAX bytes must keep working — the limit rejects
    // above the width, not at it.
    let edge = "v".repeat(u16::MAX as usize);
    let entries = vec![("cpu_usage".to_string(), labels(&[("host", edge.as_str())]))];
    eng.resolve_series_batch(&entries).unwrap();
    eng.flush_all().unwrap();
    assert_eq!(saved.lock().unwrap().len(), 1);
}
