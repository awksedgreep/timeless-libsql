//! BlockEngine unit tests (Session 5 acceptance list):
//!   - raw → optimize → query round-trip exactness
//!   - term pruning actually SKIPS blocks (counted via a wrapper store)
//!   - LEVEL-PARTITIONED flush: level-pure blocks, one level: term each,
//!     optimize never merges across partitions, level queries read only
//!     their partition's blocks (the "level-term weakness" fix)
//!   - merge span cap respected
//!   - buffered + flushed merge correctness
//! plus codec round-trips and validation edges.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::codec::{
    decode_block, encode_block, CODEC_COLUMNAR, CODEC_COLUMNAR_V2, CODEC_RAW, CODEC_ZSTD,
    PAIRS_LEGACY, PAIRS_SHREDDED, SHRED_MAX_KEYS,
};
use super::engine::{BlockEngine, BlockEngineConfig, LogQuery};
use super::mem::MemBlockStore;
use super::{level_from_name, BlockLoc, BlockMeta, BlockStore, EncodedBlock, LogEntry};

fn entry(ts: i64, level: u8, message: &str, metadata: &[(&str, &str)]) -> LogEntry {
    LogEntry {
        ts,
        level,
        message: message.to_owned(),
        metadata: metadata
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    }
}

fn full_range_query() -> LogQuery {
    LogQuery {
        ts_min: i64::MIN + 1,
        ts_max: i64::MAX - 1,
        level: None,
        metadata_eq: Vec::new(),
        message_contains: None,
    }
}

fn config(index_keys: &[&str]) -> BlockEngineConfig {
    BlockEngineConfig {
        index_keys: index_keys.iter().map(|s| s.to_string()).collect(),
        ..BlockEngineConfig::default()
    }
}

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

#[test]
fn codec_round_trips_all_codecs() {
    // CODEC_ZSTD and CODEC_COLUMNAR stay in this loop FOREVER even
    // though optimize() no longer writes them: encode_block with the
    // legacy codecs is the retained legacy encoder path, and this
    // round-trip is the proof that existing codec-2/4 databases remain
    // decodable.
    let entries = vec![
        entry(1000, 1, "hello world", &[("service", "api"), ("path", "/x")]),
        entry(1001, 3, "boom 💥 unicode", &[]),
        entry(1005, 0, "", &[("k", "")]), // empty message + empty value
    ];
    for codec in [CODEC_RAW, CODEC_ZSTD, CODEC_COLUMNAR, CODEC_COLUMNAR_V2] {
        let (bytes, meta) = encode_block(&entries, codec, 7).unwrap();
        assert_eq!(meta.ts_min, 1000);
        assert_eq!(meta.ts_max, 1005);
        assert_eq!(meta.entry_count, 3);
        assert_eq!(meta.codec, codec);
        let back = decode_block(&bytes).unwrap();
        assert_eq!(back, entries, "codec {codec} round-trip");
    }
}

#[test]
fn codec_rejects_garbage() {
    let entries = vec![entry(1, 1, "x", &[])];
    let (bytes, _) = encode_block(&entries, CODEC_ZSTD, 7).unwrap();
    // Truncation anywhere must be an error, never a panic.
    for cut in [0, 1, 10, bytes.len() - 1] {
        assert!(decode_block(&bytes[..cut]).is_err(), "cut at {cut}");
    }
    // Bad level byte at encode time.
    let bad = vec![entry(1, 9, "x", &[])];
    assert!(encode_block(&bad, CODEC_RAW, 7).is_err());
    // Empty blocks are refused (a block with no entries has no reason
    // to exist and would break ts_min/ts_max).
    assert!(encode_block(&[], CODEC_RAW, 7).is_err());
}

// ---------------------------------------------------------------------------
// Codec-5 metadata shredding: hostile shapes + the strategy byte.
// ---------------------------------------------------------------------------

/// The strategy byte of a codec-5 block's metadata column: walk the
/// container header (4 u32 column lengths at offset 22, columns start
/// at 38) to the 4th column's first byte.
fn metadata_strategy_byte(bytes: &[u8]) -> u8 {
    let len = |i: usize| {
        u32::from_le_bytes(bytes[22 + i * 4..26 + i * 4].try_into().unwrap()) as usize
    };
    bytes[38 + len(0) + len(1) + len(2)]
}

fn rt_v2(entries: &[LogEntry], expect_strategy: u8, label: &str) {
    let (bytes, meta) = encode_block(entries, CODEC_COLUMNAR_V2, 7).unwrap();
    assert_eq!(meta.codec, CODEC_COLUMNAR_V2);
    assert_eq!(metadata_strategy_byte(&bytes), expect_strategy, "{label}: strategy byte");
    let back = decode_block(&bytes).unwrap();
    assert_eq!(&back, entries, "{label}: round-trip");
}

#[test]
fn codec5_shreds_hostile_metadata_shapes_exactly() {
    // Disjoint key sets: no two entries share a key — every per-key
    // column is a 1-dense-value column with a mostly-empty bitmap.
    rt_v2(
        &[
            entry(1, 1, "a", &[("alpha", "1")]),
            entry(2, 1, "b", &[("beta", "2")]),
            entry(3, 1, "c", &[("gamma", "3")]),
        ],
        PAIRS_SHREDDED,
        "disjoint keys",
    );

    // Empty metadata everywhere: zero keys, the shredded column is
    // just [strategy][n_keys=0].
    rt_v2(
        &[entry(1, 1, "a", &[]), entry(2, 2, "b", &[])],
        PAIRS_SHREDDED,
        "all empty",
    );

    // Unicode keys AND values (multi-byte, emoji, RTL), plus an empty
    // value and a key present in only some entries. Pair order is the
    // canonical BYTE order the engines produce (Arabic "مفتاح" starts
    // 0xD9.., Japanese "サービス" starts 0xE3.. — so Arabic sorts first).
    rt_v2(
        &[
            entry(1, 1, "m", &[("مفتاح", "قيمة"), ("サービス", "決済🚀")]),
            entry(2, 1, "n", &[("サービス", "")]),
            entry(3, 1, "o", &[]),
        ],
        PAIRS_SHREDDED,
        "unicode",
    );

    // Single entry.
    rt_v2(
        &[entry(9, 3, "solo", &[("k1", "v1"), ("k2", "v2")])],
        PAIRS_SHREDDED,
        "single entry",
    );

    // All entries carry the SAME pairs: bitmaps are all-ones and the
    // per-key value columns should dictionary/RLE down to nearly
    // nothing — the headline case.
    let same: Vec<LogEntry> = (0..500)
        .map(|i| entry(i, 1, "steady", &[("service", "api"), ("status", "200")]))
        .collect();
    rt_v2(&same, PAIRS_SHREDDED, "all same pairs");
}

#[test]
fn codec5_key_explosion_falls_back_to_legacy() {
    // 65+ distinct keys across the block (> SHRED_MAX_KEYS = 64):
    // shredding would pay per-key fixed costs with no repetition to
    // exploit, so the encoder must keep the legacy bytes verbatim —
    // and still round-trip exactly.
    let entries: Vec<LogEntry> = (0..(SHRED_MAX_KEYS as i64 + 1))
        .map(|i| entry(i, 1, "kaboom", &[(&format!("key-{i:03}") as &str, "v")]))
        .collect();
    assert_eq!(
        entries
            .iter()
            .flat_map(|e| e.metadata.iter().map(|(k, _)| k.clone()))
            .collect::<std::collections::HashSet<_>>()
            .len(),
        SHRED_MAX_KEYS + 1
    );
    rt_v2(&entries, PAIRS_LEGACY, "key explosion");

    // Exactly at the cap: still shredded.
    let at_cap: Vec<LogEntry> = (0..SHRED_MAX_KEYS as i64)
        .map(|i| entry(i, 1, "ok", &[(&format!("key-{i:03}") as &str, "v")]))
        .collect();
    rt_v2(&at_cap, PAIRS_SHREDDED, "at the cap");
}

#[test]
fn codec5_non_canonical_pairs_fall_back_to_legacy_and_stay_exact() {
    // The engines canonicalize (sort + dedup) at push(), so encode
    // only ever sees sorted pair lists — but encode_block is public
    // API, and the shredded form can only reproduce CANONICAL input.
    // Unsorted (or duplicate-key) pairs must therefore take the legacy
    // path and round-trip bit-identically, out-of-order pairs and all.
    let entries = vec![
        entry(1, 1, "x", &[("zulu", "1"), ("alpha", "2")]), // unsorted
        entry(2, 1, "y", &[("dup", "a"), ("dup", "b")]),    // duplicate key
    ];
    rt_v2(&entries, PAIRS_LEGACY, "non-canonical pairs");
}

#[test]
fn level_names_are_strict() {
    assert_eq!(level_from_name("debug").unwrap(), 0);
    assert_eq!(level_from_name("info").unwrap(), 1);
    assert_eq!(level_from_name("warning").unwrap(), 2);
    assert_eq!(level_from_name("error").unwrap(), 3);
    assert!(level_from_name("fatal").is_err());
    assert!(level_from_name("INFO").is_err()); // no case folding: strict
}

// ---------------------------------------------------------------------------
// Round-trip: raw → optimize → query exactness
// ---------------------------------------------------------------------------

#[test]
fn raw_optimize_query_round_trip_is_exact() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&["service"])).unwrap();

    let mut expect = Vec::new();
    for i in 0..100i64 {
        let e = entry(
            1_000 + i,
            (i % 4) as u8,
            &format!("message number {i}"),
            &[("service", if i % 2 == 0 { "api" } else { "web" })],
        );
        expect.push(e.clone());
        engine.push(e).unwrap();
    }

    // Queryable BEFORE flush (buffer path)...
    assert_eq!(engine.query(&full_range_query()).unwrap(), expect);
    // ...identical after flush. The buffer holds all four levels, so
    // the level-partitioned flush writes FOUR level-pure raw blocks.
    assert_eq!(engine.flush().unwrap(), 100);
    assert_eq!(engine.stats().0, 4, "one raw block per level present");
    assert_eq!(engine.query(&full_range_query()).unwrap(), expect);
    // ...and identical after optimize (zstd block path). Each level
    // partition compacts separately: 4 raw → 4 zstd, never merged
    // across levels.
    let (removed, written) = engine.optimize().unwrap();
    assert_eq!((removed, written), (4, 4));
    assert_eq!(engine.query(&full_range_query()).unwrap(), expect);
    let (blocks, raw, buffered) = engine.stats();
    assert_eq!((blocks, raw, buffered), (4, 0, 0));

    // Filtered queries are exact too.
    let q = LogQuery {
        level: Some(3),
        metadata_eq: vec![("service".into(), "web".into())],
        ..full_range_query()
    };
    let got = engine.query(&q).unwrap();
    let want: Vec<LogEntry> = expect
        .iter()
        .filter(|e| e.level == 3 && e.meta_value("service") == Some("web"))
        .cloned()
        .collect();
    assert!(!want.is_empty());
    assert_eq!(got, want);

    // Substring filter (scan-only path).
    let q = LogQuery {
        message_contains: Some("number 42".into()),
        ..full_range_query()
    };
    assert_eq!(engine.query(&q).unwrap(), vec![expect[42].clone()]);
}

// ---------------------------------------------------------------------------
// optimize() output codec: prove the compacted blocks carry codec
// byte 5 (CODEC_COLUMNAR_V2) on disk AND decode back to the exact
// entries. A delegating wrapper over a shared MemBlockStore lets the
// test inspect the store after the engine is done with it.
// ---------------------------------------------------------------------------

struct SharedStore(Arc<MemBlockStore>);

impl BlockStore for SharedStore {
    fn put_block(&self, block: &EncodedBlock) -> Result<BlockLoc, String> {
        self.0.put_block(block)
    }
    fn replace_blocks(
        &self,
        add: &[EncodedBlock],
        remove: &[BlockLoc],
        on_committed: &mut dyn FnMut(&[BlockLoc]),
    ) -> Result<Vec<BlockLoc>, String> {
        self.0.replace_blocks(add, remove, on_committed)
    }
    fn read_block(&self, loc: &BlockLoc) -> Result<Vec<u8>, String> {
        self.0.read_block(loc)
    }
    fn delete_blocks(&self, locs: &[BlockLoc]) -> Vec<String> {
        self.0.delete_blocks(locs)
    }
    fn scan(&self) -> Result<Vec<(BlockMeta, BlockLoc)>, String> {
        self.0.scan()
    }
    fn query_terms(
        &self,
        terms: &[String],
        ts_min: i64,
        ts_max: i64,
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        self.0.query_terms(terms, ts_min, ts_max)
    }
    fn save_meta(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.0.save_meta(key, value)
    }
    fn load_meta(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.0.load_meta(key)
    }
}

#[test]
fn optimize_writes_codec_5_blocks_that_decode_exactly() {
    let shared = Arc::new(MemBlockStore::new());
    let engine =
        BlockEngine::new(Box::new(SharedStore(Arc::clone(&shared))), config(&["service"])).unwrap();

    let mut expect = Vec::new();
    for i in 0..200i64 {
        let e = entry(
            5_000 + i,
            (i % 4) as u8,
            &format!("payload {i} with some repetitive structure"),
            &[("service", if i % 2 == 0 { "api" } else { "web" })],
        );
        expect.push(e.clone());
        engine.push(e).unwrap();
    }
    engine.flush().unwrap();
    engine.optimize().unwrap();

    // Every persisted block is codec 5 now — in the store metadata AND
    // in the payload's own codec byte (offset 1) — and the payloads
    // decode to exactly what was pushed.
    let mut decoded = Vec::new();
    let scanned = shared.scan().unwrap();
    assert!(!scanned.is_empty());
    for (meta, loc) in scanned {
        assert_eq!(meta.codec, CODEC_COLUMNAR_V2, "store meta codec byte");
        let bytes = shared.read_block(&loc).unwrap();
        assert_eq!(bytes[1], CODEC_COLUMNAR_V2, "payload codec byte");
        decoded.extend(decode_block(&bytes).unwrap());
    }
    decoded.sort_by_key(|e| e.ts);
    assert_eq!(decoded, expect, "codec-5 optimize output round-trips");
}

// ---------------------------------------------------------------------------
// Term pruning: a store wrapper that counts read_block calls proves
// non-matching blocks are never even read, let alone decompressed.
// ---------------------------------------------------------------------------

struct CountingStore {
    inner: MemBlockStore,
    reads: Arc<AtomicUsize>,
}

impl BlockStore for CountingStore {
    fn put_block(&self, block: &EncodedBlock) -> Result<BlockLoc, String> {
        self.inner.put_block(block)
    }
    fn replace_blocks(
        &self,
        add: &[EncodedBlock],
        remove: &[BlockLoc],
        on_committed: &mut dyn FnMut(&[BlockLoc]),
    ) -> Result<Vec<BlockLoc>, String> {
        self.inner.replace_blocks(add, remove, on_committed)
    }
    fn read_block(&self, loc: &BlockLoc) -> Result<Vec<u8>, String> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read_block(loc)
    }
    fn delete_blocks(&self, locs: &[BlockLoc]) -> Vec<String> {
        self.inner.delete_blocks(locs)
    }
    fn scan(&self) -> Result<Vec<(BlockMeta, BlockLoc)>, String> {
        self.inner.scan()
    }
    fn query_terms(
        &self,
        terms: &[String],
        ts_min: i64,
        ts_max: i64,
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        self.inner.query_terms(terms, ts_min, ts_max)
    }
    fn save_meta(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.inner.save_meta(key, value)
    }
    fn load_meta(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.inner.load_meta(key)
    }
}

#[test]
fn term_index_skips_blocks() {
    let reads = Arc::new(AtomicUsize::new(0));
    let store = CountingStore {
        inner: MemBlockStore::new(),
        reads: Arc::clone(&reads),
    };
    let engine = BlockEngine::new(Box::new(store), config(&["service"])).unwrap();

    // Three blocks, one service each (flush between pushes → one block
    // per service).
    for (base, svc) in [(1_000, "api"), (2_000, "web"), (3_000, "db")] {
        for i in 0..10 {
            engine
                .push(entry(base + i, 1, "m", &[("service", svc)]))
                .unwrap();
        }
        engine.flush().unwrap();
    }

    reads.store(0, Ordering::SeqCst);
    let q = LogQuery {
        metadata_eq: vec![("service".into(), "web".into())],
        ..full_range_query()
    };
    let got = engine.query(&q).unwrap();
    assert_eq!(got.len(), 10);
    assert!(got.iter().all(|e| e.meta_value("service") == Some("web")));
    // THE assertion: only the one matching block was read.
    assert_eq!(reads.load(Ordering::SeqCst), 1);

    // Level term prunes the same way: only one block has errors.
    for i in 0..5 {
        engine
            .push(entry(5_000 + i, 3, "err", &[("service", "api")]))
            .unwrap();
    }
    engine.flush().unwrap();
    reads.store(0, Ordering::SeqCst);
    let q = LogQuery {
        level: Some(3),
        ..full_range_query()
    };
    assert_eq!(engine.query(&q).unwrap().len(), 5);
    assert_eq!(reads.load(Ordering::SeqCst), 1);

    // Time-range pruning without terms: only the block overlapping the
    // window is read.
    reads.store(0, Ordering::SeqCst);
    let q = LogQuery {
        ts_min: 2_000,
        ts_max: 2_500,
        ..full_range_query()
    };
    assert_eq!(engine.query(&q).unwrap().len(), 10);
    assert_eq!(reads.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------
// Level partitioning (the "level-term weakness" fix): flush writes
// level-pure blocks, optimize never merges across levels, and level
// queries therefore read ONLY their level's blocks.
// ---------------------------------------------------------------------------

/// Wrapper that records the term set of every block persisted, whether
/// via put_block/put_blocks (flush) or replace_blocks (optimize) — the
/// direct way to assert level purity of what actually hit the store.
struct TermCapturingStore {
    inner: MemBlockStore,
    put_terms: Arc<Mutex<Vec<Vec<String>>>>,
    replace_terms: Arc<Mutex<Vec<Vec<String>>>>,
}

impl BlockStore for TermCapturingStore {
    fn put_block(&self, block: &EncodedBlock) -> Result<BlockLoc, String> {
        // The default put_blocks loops put_block, so recording here
        // captures batched flushes too.
        self.put_terms.lock().unwrap().push(block.terms.clone());
        self.inner.put_block(block)
    }
    fn replace_blocks(
        &self,
        add: &[EncodedBlock],
        remove: &[BlockLoc],
        on_committed: &mut dyn FnMut(&[BlockLoc]),
    ) -> Result<Vec<BlockLoc>, String> {
        let mut rec = self.replace_terms.lock().unwrap();
        for b in add {
            rec.push(b.terms.clone());
        }
        drop(rec);
        self.inner.replace_blocks(add, remove, on_committed)
    }
    fn read_block(&self, loc: &BlockLoc) -> Result<Vec<u8>, String> {
        self.inner.read_block(loc)
    }
    fn delete_blocks(&self, locs: &[BlockLoc]) -> Vec<String> {
        self.inner.delete_blocks(locs)
    }
    fn scan(&self) -> Result<Vec<(BlockMeta, BlockLoc)>, String> {
        self.inner.scan()
    }
    fn query_terms(
        &self,
        terms: &[String],
        ts_min: i64,
        ts_max: i64,
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        self.inner.query_terms(terms, ts_min, ts_max)
    }
    fn save_meta(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.inner.save_meta(key, value)
    }
    fn load_meta(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.inner.load_meta(key)
    }
}

fn level_terms_of(terms: &[String]) -> Vec<&String> {
    terms.iter().filter(|t| t.starts_with("level:")).collect()
}

#[test]
fn flush_writes_level_pure_blocks_with_single_level_term() {
    let put_terms = Arc::new(Mutex::new(Vec::new()));
    let store = TermCapturingStore {
        inner: MemBlockStore::new(),
        put_terms: Arc::clone(&put_terms),
        replace_terms: Arc::new(Mutex::new(Vec::new())),
    };
    let engine = BlockEngine::new(Box::new(store), config(&["service"])).unwrap();

    // Interleave all four levels in one buffer — the pre-fix flush
    // would have written ONE block carrying all four level: terms.
    let mut expect = Vec::new();
    for i in 0..40i64 {
        let e = entry(1_000 + i, (i % 4) as u8, &format!("m{i}"), &[("service", "api")]);
        expect.push(e.clone());
        engine.push(e).unwrap();
    }
    engine.flush().unwrap();

    // One block per level present, each with EXACTLY one level: term.
    let recorded = put_terms.lock().unwrap();
    assert_eq!(recorded.len(), 4, "one block per level present");
    for terms in recorded.iter() {
        let lt = level_terms_of(terms);
        assert_eq!(lt.len(), 1, "level-pure block must emit one level: term, got {terms:?}");
        // Non-level terms still present (metadata indexing unchanged).
        assert!(terms.iter().any(|t| t == "service:api"));
    }
    drop(recorded);

    // The partitioned layout is invisible to queries: exact round-trip.
    assert_eq!(engine.query(&full_range_query()).unwrap(), expect);
}

#[test]
fn optimize_never_merges_across_levels() {
    let replace_terms = Arc::new(Mutex::new(Vec::new()));
    let store = Arc::new(MemBlockStore::new());

    // Three flushes, each containing info AND error entries → six pure
    // raw blocks (3 info + 3 error), interleaved in time.
    struct Shared(Arc<MemBlockStore>, Arc<Mutex<Vec<Vec<String>>>>);
    impl BlockStore for Shared {
        fn put_block(&self, b: &EncodedBlock) -> Result<BlockLoc, String> {
            self.0.put_block(b)
        }
        fn replace_blocks(
            &self,
            a: &[EncodedBlock],
            r: &[BlockLoc],
            c: &mut dyn FnMut(&[BlockLoc]),
        ) -> Result<Vec<BlockLoc>, String> {
            let mut rec = self.1.lock().unwrap();
            for b in a {
                rec.push(b.terms.clone());
            }
            drop(rec);
            self.0.replace_blocks(a, r, c)
        }
        fn read_block(&self, l: &BlockLoc) -> Result<Vec<u8>, String> {
            self.0.read_block(l)
        }
        fn delete_blocks(&self, l: &[BlockLoc]) -> Vec<String> {
            self.0.delete_blocks(l)
        }
        fn scan(&self) -> Result<Vec<(BlockMeta, BlockLoc)>, String> {
            self.0.scan()
        }
        fn query_terms(
            &self,
            t: &[String],
            lo: i64,
            hi: i64,
        ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
            self.0.query_terms(t, lo, hi)
        }
        fn save_meta(&self, k: &str, v: &[u8]) -> Result<(), String> {
            self.0.save_meta(k, v)
        }
        fn load_meta(&self, k: &str) -> Result<Option<Vec<u8>>, String> {
            self.0.load_meta(k)
        }
    }

    let engine = BlockEngine::new(
        Box::new(Shared(Arc::clone(&store), Arc::clone(&replace_terms))),
        config(&[]),
    )
    .unwrap();
    for base in [1_000i64, 2_000, 3_000] {
        for i in 0..10 {
            engine.push(entry(base + i, 1, "info msg", &[])).unwrap();
            engine.push(entry(base + i, 3, "error msg", &[])).unwrap();
        }
        engine.flush().unwrap();
    }
    assert_eq!(engine.stats().0, 6, "3 flushes x 2 levels = 6 pure blocks");

    // The time ranges of info and error blocks OVERLAP EXACTLY — the
    // old level-blind grouping would happily merge them. Partitioned
    // optimize must merge 3 info → 1 and 3 error → 1, never across.
    let (removed, written) = engine.optimize().unwrap();
    assert_eq!((removed, written), (6, 2));
    for terms in replace_terms.lock().unwrap().iter() {
        assert_eq!(
            level_terms_of(terms).len(),
            1,
            "merged block crossed level partitions: {terms:?}"
        );
    }
    assert_eq!(engine.query(&full_range_query()).unwrap().len(), 60);
    drop(engine);

    // Recovery proof: a fresh engine derives partitions from the
    // level: posting lists. If it misclassified the two pure blocks as
    // mixed they would share a bucket and a second optimize would merge
    // them (2 removed, 1 written); correct derivation leaves two lone
    // small zstd blocks alone.
    let engine2 = BlockEngine::new(
        Box::new(Shared(store, Arc::new(Mutex::new(Vec::new())))),
        config(&[]),
    )
    .unwrap();
    assert_eq!(
        engine2.optimize().unwrap(),
        (0, 0),
        "recovered partitions must keep info/error blocks apart"
    );
}

#[test]
fn legacy_mixed_blocks_never_merge_with_pure_ones() {
    // Simulate a block written BEFORE partitioning: encode a level-
    // mixed batch and put it directly, with both level: terms — exactly
    // what the old flush persisted. Codec version is unchanged, so this
    // is byte-for-byte what an existing db contains.
    let store = Arc::new(MemBlockStore::new());
    let mixed_entries = vec![
        entry(1_000, 1, "old info", &[]),
        entry(1_001, 3, "old error", &[]),
    ];
    let (data, meta) = encode_block(&mixed_entries, CODEC_RAW, 7).unwrap();
    store
        .put_block(&EncodedBlock {
            meta,
            data,
            terms: vec!["level:error".into(), "level:info".into()],
        })
        .unwrap();

    struct Shared(Arc<MemBlockStore>);
    impl BlockStore for Shared {
        fn put_block(&self, b: &EncodedBlock) -> Result<BlockLoc, String> {
            self.0.put_block(b)
        }
        fn replace_blocks(
            &self,
            a: &[EncodedBlock],
            r: &[BlockLoc],
            c: &mut dyn FnMut(&[BlockLoc]),
        ) -> Result<Vec<BlockLoc>, String> {
            self.0.replace_blocks(a, r, c)
        }
        fn read_block(&self, l: &BlockLoc) -> Result<Vec<u8>, String> {
            self.0.read_block(l)
        }
        fn delete_blocks(&self, l: &[BlockLoc]) -> Vec<String> {
            self.0.delete_blocks(l)
        }
        fn scan(&self) -> Result<Vec<(BlockMeta, BlockLoc)>, String> {
            self.0.scan()
        }
        fn query_terms(
            &self,
            t: &[String],
            lo: i64,
            hi: i64,
        ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
            self.0.query_terms(t, lo, hi)
        }
        fn save_meta(&self, k: &str, v: &[u8]) -> Result<(), String> {
            self.0.save_meta(k, v)
        }
        fn load_meta(&self, k: &str) -> Result<Option<Vec<u8>>, String> {
            self.0.load_meta(k)
        }
    }

    // Recovery classifies the legacy block as mixed (two level: terms).
    let engine = BlockEngine::new(Box::new(Shared(store)), config(&[])).unwrap();
    // Add an overlapping-in-time PURE info block.
    for i in 0..5 {
        engine.push(entry(1_000 + i, 1, "new info", &[])).unwrap();
    }
    engine.flush().unwrap();
    assert_eq!(engine.stats().0, 2);

    // Both blocks are RAW and time-adjacent, but live in different
    // partitions (mixed vs info-pure): optimize must rewrite each to
    // zstd SEPARATELY, never combining them.
    let (removed, written) = engine.optimize().unwrap();
    assert_eq!(
        (removed, written),
        (2, 2),
        "mixed legacy block must not merge with a pure block"
    );
    // All seven entries still there.
    assert_eq!(engine.query(&full_range_query()).unwrap().len(), 7);
}

#[test]
fn level_query_reads_only_that_levels_blocks() {
    // THE regression test for the measured problem: with level-mixed
    // flushes every block carried level:error and a level=error query
    // decompressed all of them (356ms/1M in bench-logs — slower than a
    // table scan). With partitioned flushes it must read ONLY the
    // error-pure blocks.
    let reads = Arc::new(AtomicUsize::new(0));
    let store = CountingStore {
        inner: MemBlockStore::new(),
        reads: Arc::clone(&reads),
    };
    let engine = BlockEngine::new(Box::new(store), config(&[])).unwrap();

    // Four flushes of realistic mixed traffic: mostly info + debug,
    // errors in only some entries. Pre-fix layout: 4 blocks, all
    // carrying level:error. Post-fix: 12 pure blocks, 4 of them error.
    for base in [1_000i64, 2_000, 3_000, 4_000] {
        for i in 0..20 {
            engine.push(entry(base + i, 1, "info", &[])).unwrap();
            engine.push(entry(base + i, 0, "debug", &[])).unwrap();
            if i % 5 == 0 {
                engine.push(entry(base + i, 3, "error", &[])).unwrap();
            }
        }
        engine.flush().unwrap();
    }
    assert_eq!(engine.stats().0, 12, "4 flushes x 3 levels present");

    reads.store(0, Ordering::SeqCst);
    let q = LogQuery {
        level: Some(3),
        ..full_range_query()
    };
    assert_eq!(engine.query(&q).unwrap().len(), 16);
    // THE assertion: only the 4 error-pure blocks were read; the 8
    // info/debug blocks were pruned by the posting-list intersection
    // without a single byte of their payloads being touched.
    assert_eq!(reads.load(Ordering::SeqCst), 4);

    // Same after optimize (merges happen within partitions only, so
    // the error partition compacts to 1 block → 1 read).
    engine.optimize().unwrap();
    reads.store(0, Ordering::SeqCst);
    assert_eq!(engine.query(&q).unwrap().len(), 16);
    assert_eq!(reads.load(Ordering::SeqCst), 1);
}

// ---------------------------------------------------------------------------
// Merge cap
// ---------------------------------------------------------------------------

#[test]
fn merge_respects_ts_span_cap() {
    // Cap of 100 ts units; three small raw blocks at ts ~0, ~50, ~1000.
    // Blocks 1+2 fit inside one 100-unit span, block 3 must NOT merge
    // with them (0..=1009 would straddle a retention boundary).
    let cfg = BlockEngineConfig {
        merge_max_ts_span: 100,
        merge_target_entries: 1_000_000, // entry count never the limiter here
        ..config(&[])
    };
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), cfg).unwrap();

    for base in [0i64, 50, 1_000] {
        for i in 0..10 {
            engine.push(entry(base + i, 1, "m", &[])).unwrap();
        }
        engine.flush().unwrap();
    }
    let (removed, written) = engine.optimize().unwrap();
    assert_eq!(removed, 3);
    assert_eq!(written, 2, "cap must split the merge into two blocks");

    // And with an uncapped config the same layout merges into ONE.
    let engine2 =
        BlockEngine::new(Box::new(MemBlockStore::new()), config(&[])).unwrap();
    for base in [0i64, 50, 1_000] {
        for i in 0..10 {
            engine2.push(entry(base + i, 1, "m", &[])).unwrap();
        }
        engine2.flush().unwrap();
    }
    let (removed, written) = engine2.optimize().unwrap();
    assert_eq!((removed, written), (3, 1));

    // Data survives both shapes intact.
    assert_eq!(engine.query(&full_range_query()).unwrap().len(), 30);
    assert_eq!(engine2.query(&full_range_query()).unwrap().len(), 30);
}

#[test]
fn optimize_leaves_lone_small_zstd_blocks_alone() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&[])).unwrap();
    for i in 0..10 {
        engine.push(entry(i, 1, "m", &[])).unwrap();
    }
    engine.flush().unwrap();
    assert_eq!(engine.optimize().unwrap(), (1, 1)); // raw → zstd
    // Second optimize: the lone small zstd block is NOT rewritten
    // (write amplification for zero gain).
    assert_eq!(engine.optimize().unwrap(), (0, 0));
}

// ---------------------------------------------------------------------------
// Buffer + flushed merge
// ---------------------------------------------------------------------------

#[test]
fn buffered_and_flushed_entries_merge_sorted() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&[])).unwrap();

    // Flushed: ts 10, 30. Buffered: ts 20, 40. Query must interleave.
    engine.push(entry(10, 1, "flushed-10", &[])).unwrap();
    engine.push(entry(30, 1, "flushed-30", &[])).unwrap();
    engine.flush().unwrap();
    engine.push(entry(20, 1, "buffered-20", &[])).unwrap();
    engine.push(entry(40, 1, "buffered-40", &[])).unwrap();

    let got = engine.query(&full_range_query()).unwrap();
    let msgs: Vec<&str> = got.iter().map(|e| e.message.as_str()).collect();
    assert_eq!(msgs, ["flushed-10", "buffered-20", "flushed-30", "buffered-40"]);

    // Filters apply to buffered entries too.
    let q = LogQuery {
        ts_min: 15,
        ts_max: 35,
        ..full_range_query()
    };
    let got = engine.query(&q).unwrap();
    let msgs: Vec<&str> = got.iter().map(|e| e.message.as_str()).collect();
    assert_eq!(msgs, ["buffered-20", "flushed-30"]);
}

// ---------------------------------------------------------------------------
// Prune, recovery, validation odds and ends
// ---------------------------------------------------------------------------

#[test]
fn prune_deletes_expired_blocks_and_buffer_entries() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&[])).unwrap();
    for i in 0..10 {
        engine.push(entry(1_000 + i, 1, "old", &[])).unwrap();
    }
    engine.flush().unwrap();
    for i in 0..10 {
        engine.push(entry(9_000 + i, 1, "new", &[])).unwrap();
    }
    engine.flush().unwrap();
    engine.push(entry(500, 1, "old-buffered", &[])).unwrap();
    engine.push(entry(9_500, 1, "new-buffered", &[])).unwrap();

    assert_eq!(engine.prune(5_000).unwrap(), 1); // one whole block gone
    let got = engine.query(&full_range_query()).unwrap();
    assert_eq!(got.len(), 11);
    assert!(got.iter().all(|e| e.ts >= 5_000));
}

#[test]
fn recovery_rebuilds_index_from_scan() {
    // Same store, two engine generations — simulates vtab reconnect.
    let store = Arc::new(MemBlockStore::new());

    struct SharedStore(Arc<MemBlockStore>);
    impl BlockStore for SharedStore {
        fn put_block(&self, b: &EncodedBlock) -> Result<BlockLoc, String> {
            self.0.put_block(b)
        }
        fn replace_blocks(
            &self,
            a: &[EncodedBlock],
            r: &[BlockLoc],
            c: &mut dyn FnMut(&[BlockLoc]),
        ) -> Result<Vec<BlockLoc>, String> {
            self.0.replace_blocks(a, r, c)
        }
        fn read_block(&self, l: &BlockLoc) -> Result<Vec<u8>, String> {
            self.0.read_block(l)
        }
        fn delete_blocks(&self, l: &[BlockLoc]) -> Vec<String> {
            self.0.delete_blocks(l)
        }
        fn scan(&self) -> Result<Vec<(BlockMeta, BlockLoc)>, String> {
            self.0.scan()
        }
        fn query_terms(
            &self,
            t: &[String],
            lo: i64,
            hi: i64,
        ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
            self.0.query_terms(t, lo, hi)
        }
        fn save_meta(&self, k: &str, v: &[u8]) -> Result<(), String> {
            self.0.save_meta(k, v)
        }
        fn load_meta(&self, k: &str) -> Result<Option<Vec<u8>>, String> {
            self.0.load_meta(k)
        }
    }

    let engine = BlockEngine::new(
        Box::new(SharedStore(Arc::clone(&store))),
        config(&["service"]),
    )
    .unwrap();
    for i in 0..20 {
        engine
            .push(entry(1_000 + i, 1, &format!("m{i}"), &[("service", "api")]))
            .unwrap();
    }
    engine.flush().unwrap();
    engine.optimize().unwrap();
    let want = engine.query(&full_range_query()).unwrap();
    drop(engine);

    // "Reopen": a fresh engine over the same store must see everything
    // (buffered entries are gone — that is the documented POC contract,
    // same as metrics: durability begins at flush).
    let engine2 = BlockEngine::new(
        Box::new(SharedStore(store)),
        config(&["service"]),
    )
    .unwrap();
    assert_eq!(engine2.query(&full_range_query()).unwrap(), want);
    // prune/optimize planning works off the recovered index too.
    assert_eq!(engine2.stats().0, 1);
}

#[test]
fn push_validates_level_and_canonicalizes_metadata() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&[])).unwrap();
    assert!(engine.push(entry(1, 4, "bad", &[])).is_err());

    // Unsorted + duplicate keys: sorted, last duplicate wins.
    engine
        .push(entry(1, 1, "m", &[("z", "1"), ("a", "2"), ("z", "3")]))
        .unwrap();
    let got = engine.query(&full_range_query()).unwrap();
    assert_eq!(
        got[0].metadata,
        vec![("a".to_string(), "2".to_string()), ("z".to_string(), "3".to_string())]
    );
}

// ---------------------------------------------------------------------------
// Transaction journal (PLAN.md R5)
//
// Scope note: MemBlockStore is NOT transactional, so these tests verify
// the ENGINE-MEMORY half of rollback (buffer truncation/restore, index
// add/remove/restore, journal dedup). The store-side half — block/term
// rows actually vanishing and reappearing with the host transaction —
// only exists over real SQLite and is asserted end-to-end by
// tests/cli.sh (rollback sections) and the oracle property test.
// ---------------------------------------------------------------------------

#[test]
fn txn_rollback_discards_buffered_entries() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&[])).unwrap();
    engine.push(entry(1, 1, "pre-1", &[])).unwrap();
    engine.push(entry(2, 2, "pre-2", &[])).unwrap();

    engine.txn_begin();
    engine.push(entry(3, 1, "txn-1", &[])).unwrap();
    engine.push(entry(4, 3, "txn-2", &[])).unwrap();
    assert_eq!(engine.buffered_count(), 4);
    engine.txn_rollback();

    // Only the pre-txn entries remain, and they are still queryable.
    assert_eq!(engine.buffered_count(), 2);
    let got = engine.query(&full_range_query()).unwrap();
    assert_eq!(
        got.iter().map(|e| e.message.as_str()).collect::<Vec<_>>(),
        vec!["pre-1", "pre-2"]
    );
}

#[test]
fn txn_commit_keeps_everything() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&[])).unwrap();
    engine.txn_begin();
    engine.push(entry(1, 1, "a", &[])).unwrap();
    engine.flush().unwrap();
    engine.push(entry(2, 1, "b", &[])).unwrap();
    engine.txn_commit();

    assert_eq!(engine.stats(), (1, 1, 1)); // 1 block (raw), 1 buffered
    assert_eq!(engine.query(&full_range_query()).unwrap().len(), 2);
}

#[test]
fn txn_rollback_restores_pretxn_entries_drained_by_intra_txn_flush() {
    // THE R5 nightmare case: entries buffered by COMMITTED statements
    // get drained into a block by a flush INSIDE a later transaction.
    // The block row rolls back with the host txn — without the journal
    // `saved` machinery those committed entries would be silently lost.
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&[])).unwrap();
    engine.push(entry(1, 1, "pre-1", &[])).unwrap();
    engine.push(entry(2, 3, "pre-2", &[])).unwrap();

    engine.txn_begin();
    engine.push(entry(3, 1, "txn-1", &[])).unwrap();
    engine.flush().unwrap(); // drains ALL 3 into level-pure blocks
    assert_eq!(engine.buffered_count(), 0);
    assert!(engine.stats().0 > 0);
    engine.txn_rollback();

    // Index entries for the intra-txn blocks are gone (their rows
    // would be too, over a real store — MemBlockStore keeps them, so
    // no query assertion here; cli.sh proves the query side) and the
    // pre-txn entries are back in the buffer...
    assert_eq!(engine.stats().0, 0);
    assert_eq!(engine.buffered_count(), 2);
    // ...and a subsequent (committed) flush persists exactly them.
    engine.flush().unwrap();
    assert_eq!(engine.buffered_count(), 0);
    assert_eq!(engine.stats().0, 2); // one info-pure + one error-pure block
}

#[test]
fn txn_rollback_undoes_optimize_index_swap() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&[])).unwrap();
    // Two committed raw flushes → two raw info-pure blocks.
    engine.push(entry(1, 1, "a", &[])).unwrap();
    engine.flush().unwrap();
    engine.push(entry(2, 1, "b", &[])).unwrap();
    engine.flush().unwrap();
    assert_eq!(engine.stats(), (2, 2, 0));

    engine.txn_begin();
    let (removed, written) = engine.optimize().unwrap();
    assert_eq!((removed, written), (2, 1));
    assert_eq!(engine.stats(), (1, 0, 0));
    engine.txn_rollback();

    // The pre-txn raw entries are restored VERBATIM (metas, locs and
    // partition tags — IndexEntry is journaled wholesale) and the
    // merged block's entry is gone. No store reads after this point:
    // MemBlockStore is not transactional, so the restored locs dangle
    // HERE — over real SQLite the host rollback restores the rows
    // under the same ids, which cli.sh asserts end-to-end.
    assert_eq!(engine.stats(), (2, 2, 0));
}

#[test]
fn txn_rollback_undoes_prune_removals_and_buffer_retain() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&[])).unwrap();
    engine.push(entry(100, 1, "old-flushed", &[])).unwrap();
    engine.flush().unwrap();
    engine.push(entry(200, 1, "old-buffered", &[])).unwrap();
    assert_eq!(engine.stats(), (1, 1, 1));

    engine.txn_begin();
    // Cutoff above everything: drops the block AND the pre-txn
    // buffered entry (prune retains only ts >= cutoff).
    assert_eq!(engine.prune(1_000).unwrap(), 1);
    assert_eq!(engine.stats(), (0, 0, 0));
    engine.txn_rollback();

    // Index entry restored, buffered entry restored.
    assert_eq!(engine.stats(), (1, 1, 1));
}

#[test]
fn txn_add_then_remove_in_one_txn_cancels() {
    // flush + optimize inside ONE transaction: the raw blocks born in
    // the txn are consumed by the txn's own optimize. Rollback must not
    // resurrect them (their rows never survive the host rollback).
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&[])).unwrap();
    engine.txn_begin();
    engine.push(entry(1, 1, "a", &[])).unwrap();
    engine.flush().unwrap();
    engine.push(entry(2, 1, "b", &[])).unwrap();
    engine.flush().unwrap();
    engine.optimize().unwrap();
    assert_eq!(engine.stats().0, 1);
    engine.txn_rollback();

    // Nothing left: no phantom raw entries, no merged entry, no buffer.
    assert_eq!(engine.stats(), (0, 0, 0));
}
