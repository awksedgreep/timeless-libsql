//! BlockEngine unit tests (Session 5 acceptance list):
//!   - raw → optimize → query round-trip exactness
//!   - term pruning actually SKIPS blocks (counted via a wrapper store)
//!   - merge span cap respected
//!   - buffered + flushed merge correctness
//! plus codec round-trips and validation edges.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::codec::{decode_block, encode_block, CODEC_RAW, CODEC_ZSTD};
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
fn codec_round_trips_both_codecs() {
    let entries = vec![
        entry(1000, 1, "hello world", &[("service", "api"), ("path", "/x")]),
        entry(1001, 3, "boom 💥 unicode", &[]),
        entry(1005, 0, "", &[("k", "")]), // empty message + empty value
    ];
    for codec in [CODEC_RAW, CODEC_ZSTD] {
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
    // ...identical after flush (raw block path)...
    assert_eq!(engine.flush().unwrap(), 100);
    assert_eq!(engine.query(&full_range_query()).unwrap(), expect);
    // ...and identical after optimize (zstd block path).
    let (removed, written) = engine.optimize().unwrap();
    assert_eq!((removed, written), (1, 1)); // one raw block → one zstd block
    assert_eq!(engine.query(&full_range_query()).unwrap(), expect);
    let (blocks, raw, buffered) = engine.stats();
    assert_eq!((blocks, raw, buffered), (1, 0, 0));

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
    ) -> Result<Vec<BlockLoc>, String> {
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
        ) -> Result<Vec<BlockLoc>, String> {
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
