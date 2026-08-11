//! Authoritative catalog-generation publication tests.
//!
//! SQLite captures the final token in xSync and publishes it only from
//! xCommit. These engine-level tests pin the two important properties: a local
//! committed generation skips the redundant full reload, while external
//! changes still force one and a rolled-back captured token is never applied.

use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::Arc;

use timeless_core::{
    ChunkBytes, ChunkLoc, ChunkStore, EncodedChunk, Engine, ResolvedSeries, StoredChunk,
    StoredSeries,
};

#[derive(Default)]
struct CatalogState {
    series_generation: AtomicI64,
    chunk_generation: AtomicI64,
    series_loads: AtomicUsize,
    chunk_scans: AtomicUsize,
}

struct CountingCatalogStore(Arc<CatalogState>);

impl ChunkStore for CountingCatalogStore {
    fn put_chunks(&self, _chunks: &[EncodedChunk]) -> Result<Vec<ChunkLoc>, String> {
        Err("not used by catalog publication tests".into())
    }

    fn replace_chunks(
        &self,
        _add: &[EncodedChunk],
        _remove: &[ChunkLoc],
        _on_committed: &mut dyn FnMut(&[ChunkLoc]),
    ) -> Result<Vec<ChunkLoc>, String> {
        Err("not used by catalog publication tests".into())
    }

    fn read_chunk(&self, _loc: &ChunkLoc) -> Result<ChunkBytes, String> {
        Err("not used by catalog publication tests".into())
    }

    fn delete_chunks(&self, _locs: &[ChunkLoc]) -> Vec<String> {
        Vec::new()
    }

    fn scan(&self) -> Result<Vec<StoredChunk>, String> {
        self.0.chunk_scans.fetch_add(1, Ordering::SeqCst);
        Ok(Vec::new())
    }

    fn save_registry(&self, _bytes: &[u8]) -> Result<(), String> {
        Ok(())
    }

    fn load_registry(&self) -> Result<Option<Vec<u8>>, String> {
        Ok(None)
    }

    fn has_authoritative_series(&self) -> bool {
        true
    }

    fn load_series(&self) -> Result<Vec<StoredSeries>, String> {
        self.0.series_loads.fetch_add(1, Ordering::SeqCst);
        Ok(Vec::new())
    }

    fn resolve_series(
        &self,
        _name: &str,
        _labels: &[(String, String)],
    ) -> Result<ResolvedSeries, String> {
        Err("not used by catalog publication tests".into())
    }

    fn catalog_generation(&self) -> Result<Option<(i64, i64)>, String> {
        Ok(Some((
            self.0.series_generation.load(Ordering::SeqCst),
            self.0.chunk_generation.load(Ordering::SeqCst),
        )))
    }

    fn storage_stats(&self) -> (u64, usize) {
        (0, 0)
    }

    fn sweep_cache(&self) {}
}

fn engine(state: Arc<CatalogState>) -> Engine {
    Engine::with_store(
        Box::new(CountingCatalogStore(state)),
        4096,
        0,
        8,
        64 * 1024 * 1024,
        false,
    )
    .unwrap()
}

fn loads(state: &CatalogState) -> (usize, usize) {
    (
        state.series_loads.load(Ordering::SeqCst),
        state.chunk_scans.load(Ordering::SeqCst),
    )
}

#[test]
fn committed_generation_skips_reload_but_external_change_does_not() {
    let state = Arc::new(CatalogState::default());
    let engine = engine(Arc::clone(&state));

    // Construction recovers once and PRIMES the token from the same
    // snapshot (P2), so the first refresh is already a fast path.
    assert_eq!(loads(&state), (1, 1));
    engine.refresh_authoritative_state().unwrap();
    assert_eq!(loads(&state), (1, 1));

    // Model a local SQLite transaction mutating chunk rows. xSync captures
    // token 1; xCommit publishes it alongside the already-updated engine.
    engine.txn_begin();
    state.chunk_generation.store(1, Ordering::SeqCst);
    let committed = engine.capture_catalog_generation().unwrap();
    engine.txn_commit_published(committed);
    engine.refresh_authoritative_state().unwrap();
    assert_eq!(
        loads(&state),
        (1, 1),
        "a locally published generation must not reload authoritative state"
    );

    // A change not represented in this engine still invalidates the token and
    // takes the full, always-correct recovery path (this store offers no
    // append watermark, so no delta shortcut exists).
    state.chunk_generation.store(2, Ordering::SeqCst);
    engine.refresh_authoritative_state().unwrap();
    assert_eq!(loads(&state), (2, 2));
}

#[test]
fn rolled_back_captured_generation_is_not_published() {
    let state = Arc::new(CatalogState::default());
    let engine = engine(Arc::clone(&state));
    engine.refresh_authoritative_state().unwrap();
    assert_eq!(loads(&state), (1, 1), "primed token skips the redundant reload");

    engine.txn_begin();
    state.chunk_generation.store(1, Ordering::SeqCst);
    let captured = engine.capture_catalog_generation().unwrap();
    assert_eq!(captured, Some((0, 1)));

    // xRollback discards the vtab's captured token and restores the store.
    // The engine API therefore receives no publication call.
    engine.txn_rollback();
    state.chunk_generation.store(0, Ordering::SeqCst);
    engine.refresh_authoritative_state().unwrap();
    assert_eq!(
        loads(&state),
        (1, 1),
        "rollback must retain the last committed token"
    );
}
