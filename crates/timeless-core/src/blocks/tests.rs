//! BlockEngine unit tests (Session 5 acceptance list):
//!   - raw → optimize → query round-trip exactness
//!   - term pruning actually SKIPS blocks (counted via a wrapper store)
//!   - LEVEL-PARTITIONED flush: level-pure blocks, one level: term each,
//!     optimize never merges across partitions, level queries read only
//!     their partition's blocks (the "level-term weakness" fix)
//!   - merge span cap respected
//!   - buffered + flushed merge correctness
//!     plus codec round-trips and validation edges.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Barrier, Mutex};
use std::thread;
use std::time::Duration;

use super::codec::{
    decode_block, encode_block, CODEC_COLUMNAR, CODEC_COLUMNAR_V2, CODEC_RAW, CODEC_RICH_COLUMNAR,
    CODEC_RICH_RAW, CODEC_RICH_TEMPLATE, CODEC_ZSTD, PAIRS_LEGACY, PAIRS_SHREDDED, SHRED_MAX_KEYS,
};
use super::engine::{BlockEngine, BlockEngineConfig, LogQuery, LogQueryOrder};
use super::mem::MemBlockStore;
use super::{level_from_name, BlockLoc, BlockMeta, BlockStore, EncodedBlock, LogEntry};

fn entry(ts: i64, level: u8, message: &str, metadata: &[(&str, &str)]) -> LogEntry {
    LogEntry {
        ts,
        level,
        severity: None,
        message: message.to_owned(),
        metadata: metadata
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        metadata_json: None,
    }
}

fn full_range_query() -> LogQuery {
    LogQuery {
        ts_min: i64::MIN,
        ts_max: i64::MAX,
        level: None,
        severity: None,
        metadata_eq: Vec::new(),
        message_contains: None,
        message_like_prune: None,
    }
}

#[test]
fn rich_log_codecs_preserve_severity_and_typed_metadata() {
    let rich = LogEntry {
        ts: 1_785_600_000_123_456,
        level: 1,
        severity: Some("notice".into()),
        message: "typed metadata".into(),
        metadata: vec![
            ("nested".into(), "{\"ok\":true}".into()),
            ("status".into(), "202".into()),
        ],
        metadata_json: Some("{\"nested\":{\"ok\":true},\"status\":202}".into()),
    };

    for codec in [CODEC_RICH_RAW, CODEC_RICH_COLUMNAR, CODEC_RICH_TEMPLATE] {
        let (bytes, meta) = encode_block(std::slice::from_ref(&rich), codec, 7).unwrap();
        // Codec 8 measures templates against the codec-7 message column
        // and may legitimately emit 7 for a one-entry block; every other
        // request must come back verbatim.
        if codec == CODEC_RICH_TEMPLATE {
            assert!(matches!(
                meta.codec,
                CODEC_RICH_COLUMNAR | CODEC_RICH_TEMPLATE
            ));
        } else {
            assert_eq!(meta.codec, codec);
        }
        let decoded = decode_block(&bytes).unwrap();
        assert_eq!(decoded, vec![rich.clone()]);
        assert_eq!(
            decoded[0].metadata_json.as_deref(),
            Some("{\"nested\":{\"ok\":true},\"status\":202}")
        );
    }

    assert!(encode_block(std::slice::from_ref(&rich), CODEC_RAW, 7).is_err());
    assert!(encode_block(&[rich], CODEC_COLUMNAR_V2, 7).is_err());
}

#[test]
fn template_codec_wins_on_templated_messages_and_falls_back_on_noise() {
    let rich_entry = |ts: i64, message: String| LogEntry {
        ts,
        level: 1,
        severity: Some("info".into()),
        message,
        metadata: vec![("service".into(), "auth".into())],
        metadata_json: Some("{\"service\":\"auth\"}".into()),
    };

    // Similar-but-not-identical lines: the CLP sweet spot. The block
    // must come back as codec 8, be smaller than codec 7, and decode
    // bit-exact.
    let templated: Vec<LogEntry> = (0..2048)
        .map(|i| {
            rich_entry(
                1_785_600_000_000_000 + i,
                format!(
                    "user {} logged in from 10.0.{}.{} in {}ms",
                    1000 + i,
                    i % 256,
                    (i * 7) % 256,
                    i % 900
                ),
            )
        })
        .collect();
    let (tpl_bytes, tpl_meta) = encode_block(&templated, CODEC_RICH_TEMPLATE, 7).unwrap();
    assert_eq!(tpl_meta.codec, CODEC_RICH_TEMPLATE);
    let (col_bytes, _) = encode_block(&templated, CODEC_RICH_COLUMNAR, 7).unwrap();
    assert!(
        tpl_bytes.len() < col_bytes.len(),
        "codec 8 block ({}) should beat codec 7 ({})",
        tpl_bytes.len(),
        col_bytes.len()
    );
    assert_eq!(decode_block(&tpl_bytes).unwrap(), templated);

    // Near-unique high-entropy lines: the per-block gate must emit a
    // codec 7 block instead (never larger than codec 7).
    let mut state = 0x9e3779b97f4a7c15u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let noisy: Vec<LogEntry> = (0..2048)
        .map(|i| {
            let blob: String = (0..40)
                .map(|_| {
                    let c = (next() % 62) as u8;
                    (match c {
                        0..=9 => b'0' + c,
                        10..=35 => b'a' + c - 10,
                        _ => b'A' + c - 36,
                    }) as char
                })
                .collect();
            rich_entry(1_785_600_000_000_000 + i, blob)
        })
        .collect();
    let (noise_bytes, noise_meta) = encode_block(&noisy, CODEC_RICH_TEMPLATE, 7).unwrap();
    assert_eq!(noise_meta.codec, CODEC_RICH_COLUMNAR, "fallback must fire");
    assert_eq!(decode_block(&noise_bytes).unwrap(), noisy);
    let (noise_col, _) = encode_block(&noisy, CODEC_RICH_COLUMNAR, 7).unwrap();
    assert_eq!(noise_bytes.len(), noise_col.len(), "fallback == codec 7");
}

fn config(index_keys: &[&str]) -> BlockEngineConfig {
    BlockEngineConfig {
        auto_optimize_interval_flushes: 0,
        auto_optimize_budget_entries: 32_768,
        index_keys: index_keys.iter().map(|s| s.to_string()).collect(),
        ..BlockEngineConfig::default()
    }
}

const LOCK_ORDER_TIMEOUT: Duration = Duration::from_secs(2);

struct PausingFlushStore {
    inner: MemBlockStore,
    entered: Arc<Barrier>,
    release: Arc<Barrier>,
}

impl BlockStore for PausingFlushStore {
    fn put_block(&self, block: &EncodedBlock) -> Result<BlockLoc, String> {
        self.inner.put_block(block)
    }

    fn put_blocks(&self, blocks: &[EncodedBlock]) -> Result<Vec<BlockLoc>, String> {
        self.entered.wait();
        self.release.wait();
        self.inner.put_blocks(blocks)
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
fn stats_concurrent_with_flush_does_not_invert_index_and_buffer() {
    let entered = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let engine = Arc::new(
        BlockEngine::new(
            Box::new(PausingFlushStore {
                inner: MemBlockStore::new(),
                entered: Arc::clone(&entered),
                release: Arc::clone(&release),
            }),
            config(&[]),
        )
        .unwrap(),
    );
    engine.push(entry(1, 1, "buffered", &[])).unwrap();

    let (flush_done_tx, flush_done_rx) = mpsc::channel();
    let flush_engine = Arc::clone(&engine);
    let flush_thread = thread::spawn(move || {
        let _ = flush_done_tx.send(flush_engine.flush());
    });
    entered.wait();

    let (index_read_tx, index_read_rx) = mpsc::channel();
    let (stats_done_tx, stats_done_rx) = mpsc::channel();
    let stats_engine = Arc::clone(&engine);
    let stats_thread = thread::spawn(move || {
        let stats = stats_engine.stats_after_index(|| {
            let _ = index_read_tx.send(());
        });
        let _ = stats_done_tx.send(stats);
    });
    index_read_rx
        .recv_timeout(LOCK_ORDER_TIMEOUT)
        .expect("stats never reached its post-index observation point");

    release.wait();
    assert_eq!(
        flush_done_rx
            .recv_timeout(LOCK_ORDER_TIMEOUT)
            .expect("flush deadlocked waiting for the stats index guard")
            .unwrap(),
        1
    );
    stats_done_rx
        .recv_timeout(LOCK_ORDER_TIMEOUT)
        .expect("stats deadlocked waiting for the flush buffer guard");
    flush_thread.join().unwrap();
    stats_thread.join().unwrap();
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
        entry(
            1000,
            1,
            "hello world",
            &[("service", "api"), ("path", "/x")],
        ),
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
    let len =
        |i: usize| u32::from_le_bytes(bytes[22 + i * 4..26 + i * 4].try_into().unwrap()) as usize;
    bytes[38 + len(0) + len(1) + len(2)]
}

fn rt_v2(entries: &[LogEntry], expect_strategy: u8, label: &str) {
    let (bytes, meta) = encode_block(entries, CODEC_COLUMNAR_V2, 7).unwrap();
    assert_eq!(meta.codec, CODEC_COLUMNAR_V2);
    assert_eq!(
        metadata_strategy_byte(&bytes),
        expect_strategy,
        "{label}: strategy byte"
    );
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
// reindex(): making a widened index_keys allowlist retroactive.
//
// Postings are written at insert time from the allowlist, so a block carries
// postings only for the keys indexed when it was written. Widening the
// allowlist without rewriting them makes pruning on a newly indexed key skip
// every older block — the entries are still stored, but query_terms never
// returns their blocks, so a search silently loses history. That is the
// failure these tests pin.
// ---------------------------------------------------------------------------

#[test]
fn widening_index_keys_without_reindex_hides_older_blocks() {
    let store = Arc::new(MemBlockStore::new());

    // Written when only "service" was indexed.
    let old = BlockEngine::new(Box::new(SharedStore(store.clone())), config(&["service"])).unwrap();
    old.push(entry(
        10,
        3,
        "one",
        &[("service", "api"), ("host", "web-1")],
    ))
    .unwrap();
    old.flush().unwrap();
    drop(old);

    // A later process indexes "host" too.
    let new = BlockEngine::new(
        Box::new(SharedStore(store.clone())),
        config(&["service", "host"]),
    )
    .unwrap();

    let q = LogQuery {
        metadata_eq: vec![("host".into(), "web-1".into())],
        ..full_range_query()
    };

    // The block has no host: posting, so pruning drops it. This is the bug:
    // the entry exists and matches, but the query cannot see it.
    assert!(
        new.query(&q).unwrap().is_empty(),
        "expected the pre-widening block to be pruned; if this now returns the \
         entry, pruning changed and reindex may no longer be required"
    );
}

#[test]
fn reindex_makes_a_widened_allowlist_retroactive() {
    let store = Arc::new(MemBlockStore::new());

    let old = BlockEngine::new(Box::new(SharedStore(store.clone())), config(&["service"])).unwrap();
    old.push(entry(
        10,
        3,
        "one",
        &[("service", "api"), ("host", "web-1")],
    ))
    .unwrap();
    old.push(entry(
        20,
        3,
        "two",
        &[("service", "api"), ("host", "web-2")],
    ))
    .unwrap();
    old.flush().unwrap();
    drop(old);

    let new = BlockEngine::new(
        Box::new(SharedStore(store.clone())),
        config(&["service", "host"]),
    )
    .unwrap();

    let keys = vec!["service".to_string(), "host".to_string()];
    assert_eq!(
        new.reindex(&keys).unwrap(),
        1,
        "one block should be rewritten"
    );

    let q = LogQuery {
        metadata_eq: vec![("host".into(), "web-1".into())],
        ..full_range_query()
    };
    let got = new.query(&q).unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].message, "one");

    // The other value still resolves, and the key that was always indexed is
    // not damaged by the rewrite.
    let q2 = LogQuery {
        metadata_eq: vec![("host".into(), "web-2".into())],
        ..full_range_query()
    };
    assert_eq!(new.query(&q2).unwrap().len(), 1);

    let q3 = LogQuery {
        metadata_eq: vec![("service".into(), "api".into())],
        ..full_range_query()
    };
    assert_eq!(new.query(&q3).unwrap().len(), 2);
}

#[test]
fn reindex_persists_the_allowlist_and_is_idempotent() {
    let store = Arc::new(MemBlockStore::new());

    let engine =
        BlockEngine::new(Box::new(SharedStore(store.clone())), config(&["service"])).unwrap();
    engine
        .push(entry(
            10,
            3,
            "one",
            &[("service", "api"), ("host", "web-1")],
        ))
        .unwrap();
    engine.flush().unwrap();

    let keys = vec!["service".to_string(), "host".to_string()];
    engine.reindex(&keys).unwrap();

    assert_eq!(
        store.load_meta("index_keys").unwrap().as_deref(),
        Some(b"service,host".as_ref()),
        "the new allowlist must be persisted for the next connect"
    );

    // Running it again must not duplicate or drop postings.
    engine.reindex(&keys).unwrap();

    let q = LogQuery {
        metadata_eq: vec![("host".into(), "web-1".into())],
        ..full_range_query()
    };
    assert_eq!(engine.query(&q).unwrap().len(), 1);
}

#[test]
fn reindex_narrowing_drops_stale_postings() {
    // Narrowing is unsound in the other direction: a posting left behind for a
    // key the engine no longer applies would keep pruning on it.
    let store = Arc::new(MemBlockStore::new());

    let engine = BlockEngine::new(
        Box::new(SharedStore(store.clone())),
        config(&["service", "host"]),
    )
    .unwrap();
    engine
        .push(entry(
            10,
            3,
            "one",
            &[("service", "api"), ("host", "web-1")],
        ))
        .unwrap();
    engine.flush().unwrap();

    engine.reindex(&["service".to_string()]).unwrap();
    drop(engine);

    // reindex rewrites the PERSISTED allowlist; a live engine keeps the config
    // it was constructed with. Narrowing is therefore only coherent once the
    // engine is rebuilt with the narrowed set — exactly what the vtab does when
    // it reloads index_keys from _meta at connect. Until then the old config
    // would still prune on host: postings that no longer exist, which is why
    // the two must be changed together.
    let reconnected =
        BlockEngine::new(Box::new(SharedStore(store.clone())), config(&["service"])).unwrap();

    // host is no longer indexed, so nothing prunes on it and the entry is found
    // by the exact per-entry filter.
    let q = LogQuery {
        metadata_eq: vec![("host".into(), "web-1".into())],
        ..full_range_query()
    };
    assert_eq!(reconnected.query(&q).unwrap().len(), 1);

    // And the key still indexed keeps pruning correctly.
    let q2 = LogQuery {
        metadata_eq: vec![("service".into(), "api".into())],
        ..full_range_query()
    };
    assert_eq!(reconnected.query(&q2).unwrap().len(), 1);
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
        message_like_prune: None,
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
    fn replace_terms(&self, loc: &BlockLoc, terms: &[String]) -> Result<(), String> {
        self.0.replace_terms(loc, terms)
    }
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
    let engine = BlockEngine::new(
        Box::new(SharedStore(Arc::clone(&shared))),
        config(&["service"]),
    )
    .unwrap();

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
    fn query_snapshot_keeps_locations_readable(&self) -> bool {
        // Tests using this wrapper do not mutate the MemBlockStore while a
        // query is active, so locations remain stable. This also lets the
        // native-count tests distinguish metadata-only work from payload
        // reads exactly as the SQLite store does under its read snapshot.
        true
    }

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

#[test]
fn native_count_uses_metadata_when_proven_and_decodes_only_when_needed() {
    let reads = Arc::new(AtomicUsize::new(0));
    let store = CountingStore {
        inner: MemBlockStore::new(),
        reads: Arc::clone(&reads),
    };
    let engine = BlockEngine::new(
        Box::new(store),
        BlockEngineConfig {
            auto_optimize_interval_flushes: 0,
            auto_optimize_budget_entries: 32_768,
            message_trigrams: true,
            ..config(&["service"])
        },
    )
    .unwrap();

    for (ts, message, service) in [
        (100, "routine one", "api"),
        (101, "routine two", "api"),
        (102, "routine three", "api"),
    ] {
        engine
            .push(entry(ts, 1, message, &[("service", service)]))
            .unwrap();
    }
    engine.flush().unwrap();
    for (ts, message, service) in [
        (200, "request failed", "api"),
        (201, "TIMEOUT waiting for db", "db"),
        (202, "CafÉ timeout", "db"),
    ] {
        engine
            .push(entry(ts, 3, message, &[("service", service)]))
            .unwrap();
    }
    engine.flush().unwrap();
    // The buffer participates in the same exact count without being flushed.
    engine
        .push(entry(300, 3, "buffer timeout", &[("service", "db")]))
        .unwrap();

    reads.store(0, Ordering::SeqCst);
    assert_eq!(engine.count(&full_range_query()).unwrap(), 7);
    assert_eq!(reads.load(Ordering::SeqCst), 0);
    let profile = engine.profile();
    assert_eq!(profile.native_count_count, 1);
    assert_eq!(profile.native_count_metadata_blocks, 2);
    assert_eq!(profile.native_count_metadata_entries, 6);
    assert_eq!(profile.native_count_decoded_blocks, 0);
    assert_eq!(profile.native_count_decoded_entries, 0);
    assert_eq!(profile.native_count_payload_bytes_read, 0);

    // A level term selects only the pure error block. Its partition proves
    // every persisted row matches, so this remains metadata-only.
    reads.store(0, Ordering::SeqCst);
    let errors = LogQuery {
        level: Some(3),
        ..full_range_query()
    };
    assert_eq!(engine.count(&errors).unwrap(), 4);
    assert_eq!(reads.load(Ordering::SeqCst), 0);

    // The release API always supplies the canonical exact severity. Legacy
    // codecs contain only the original four names, so `error` remains proven
    // by the legacy error partition without weakening rich-codec correctness.
    reads.store(0, Ordering::SeqCst);
    let exact_legacy_errors = LogQuery {
        level: Some(3),
        severity: Some("error".into()),
        ..full_range_query()
    };
    assert_eq!(engine.count(&exact_legacy_errors).unwrap(), 4);
    assert_eq!(reads.load(Ordering::SeqCst), 0);

    // A boundary through a block, metadata equality, or exact message
    // predicate cannot be proven by BlockMeta and therefore decodes candidate
    // blocks. In every case native count agrees with the row query. ASCII
    // contains gets trigram pruning; Unicode deliberately scans both blocks
    // because byte trigrams cannot prove Unicode lowercase equivalence.
    let cases = [
        (
            LogQuery {
                ts_min: 201,
                ts_max: 300,
                level: Some(3),
                ..full_range_query()
            },
            1,
        ),
        (
            LogQuery {
                metadata_eq: vec![("service".into(), "db".into())],
                ..full_range_query()
            },
            1,
        ),
        (
            LogQuery {
                message_contains: Some("TiMeOuT".into()),
                ..full_range_query()
            },
            1,
        ),
        (
            LogQuery {
                message_contains: Some("café".into()),
                ..full_range_query()
            },
            2,
        ),
    ];
    for (query, expected_reads) in cases {
        let expected = engine.query(&query).unwrap().len() as u64;
        reads.store(0, Ordering::SeqCst);
        assert_eq!(engine.count(&query).unwrap(), expected);
        assert_eq!(reads.load(Ordering::SeqCst), expected_reads);
    }
}

#[test]
fn exact_contains_is_case_insensitive_and_safe_for_bounded_queries() {
    let engine = BlockEngine::new(
        Box::new(MemBlockStore::new()),
        BlockEngineConfig {
            auto_optimize_interval_flushes: 0,
            auto_optimize_budget_entries: 32_768,
            message_trigrams: true,
            ..config(&[])
        },
    )
    .unwrap();
    for (ts, message) in [
        (10, "ordinary"),
        (20, "first TIMEOUT"),
        (30, "second timeout"),
        (40, "CAFÉ unavailable"),
    ] {
        engine.push(entry(ts, 1, message, &[])).unwrap();
        engine.flush().unwrap();
    }

    let timeout = LogQuery {
        message_contains: Some("TimeOut".into()),
        ..full_range_query()
    };
    let got = engine
        .query_bounded(&timeout, LogQueryOrder::Desc, 1)
        .unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].message, "second timeout");

    let unicode = LogQuery {
        message_contains: Some("café".into()),
        ..full_range_query()
    };
    assert_eq!(engine.count(&unicode).unwrap(), 1);

    assert!(!BlockEngine::message_contains_trigrams("timeout").is_empty());
    assert!(BlockEngine::message_contains_trigrams("café").is_empty());
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
        let e = entry(
            1_000 + i,
            (i % 4) as u8,
            &format!("m{i}"),
            &[("service", "api")],
        );
        expect.push(e.clone());
        engine.push(e).unwrap();
    }
    engine.flush().unwrap();

    // One block per level present, each with EXACTLY one level: term.
    let recorded = put_terms.lock().unwrap();
    assert_eq!(recorded.len(), 4, "one block per level present");
    for terms in recorded.iter() {
        let lt = level_terms_of(terms);
        assert_eq!(
            lt.len(),
            1,
            "level-pure block must emit one level: term, got {terms:?}"
        );
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
        auto_optimize_interval_flushes: 0,
        auto_optimize_budget_entries: 32_768,
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
    let engine2 = BlockEngine::new(Box::new(MemBlockStore::new()), config(&[])).unwrap();
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

#[test]
fn size_tiered_optimize_bounds_repeated_tail_rewrites() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&[])).unwrap();
    let entries_per_arrival = 256i64;
    let arrivals = 40i64;

    // This is the Session 6 amplification fixture. The former append-to-tail
    // planner rewrote 144,384 source entries for these 10,240 ingested rows
    // (14.1x): 256 + 512 + ... as the compressed tail grew each call.
    for cycle in 0..arrivals {
        for row in 0..entries_per_arrival {
            let ts = cycle * entries_per_arrival + row;
            engine
                .push(entry(ts, 1, "tail rewrite payload", &[]))
                .unwrap();
        }
        engine.flush().unwrap();
        engine.optimize().unwrap();
    }

    let profile = engine.profile();
    assert_eq!(profile.optimize_raw_blocks, arrivals as u64);
    assert_eq!(profile.optimize_raw_entries, 10_240);
    // Sixteen 256-entry compressed tails first merge to 4,096; that output
    // later merges with sixteen peers to reach the terminal 8,192 block.
    assert_eq!(profile.optimize_merge_groups, 2);
    assert_eq!(profile.optimize_merge_entries, 4_096 + 8_192);
    assert_eq!(
        profile.optimize_raw_entries + profile.optimize_merge_entries,
        22_528,
        "2.2x total rewrite work, down from the pinned 14.1x baseline"
    );
    assert_eq!(engine.query(&full_range_query()).unwrap().len(), 10_240);

    let backlog = engine.optimize_backlog();
    assert_eq!(backlog.raw_blocks, 0);
    assert_eq!(backlog.merge_ready_groups, 0);
    assert_eq!(backlog.merge_deferred_blocks, 8);
    assert_eq!(backlog.merge_deferred_entries, 2_048);
    assert_eq!(engine.optimize().unwrap(), (0, 0));
}

#[test]
fn size_tiered_optimize_consolidates_just_over_target_half_tiers() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&[])).unwrap();
    for cycle in 0..30i64 {
        for row in 0..307i64 {
            engine
                .push(entry(cycle * 307 + row, 1, "uneven tier payload", &[]))
                .unwrap();
        }
        engine.flush().unwrap();
        engine.optimize().unwrap();
    }

    let profile = engine.profile();
    // Fourteen 307-entry blocks first form 4,298. Fourteen later peers can
    // then join that tier into 8,596: 105% of the target, under the bounded
    // 125% ceiling, and exactly 2x the largest input tier.
    assert_eq!(profile.optimize_merge_groups, 2);
    assert_eq!(profile.optimize_merge_entries, 4_298 + 8_596);
    assert_eq!(engine.stats().0, 3);
    assert_eq!(engine.query(&full_range_query()).unwrap().len(), 9_210);
    assert_eq!(engine.optimize_backlog().merge_ready_entries, 0);
}

#[test]
fn budgeted_optimize_bounds_each_call_and_drains_oldest_raw_groups() {
    let engine = BlockEngine::new(
        Box::new(MemBlockStore::new()),
        BlockEngineConfig {
            auto_optimize_interval_flushes: 0,
            auto_optimize_budget_entries: 32_768,
            merge_max_ts_span: 0,
            ..config(&[])
        },
    )
    .unwrap();
    for cycle in 0..4i64 {
        for _ in 0..256 {
            engine
                .push(entry(cycle, 1, &format!("cycle-{cycle}"), &[]))
                .unwrap();
        }
        engine.flush().unwrap();
    }
    assert_eq!(engine.optimize_backlog().raw_entries, 1_024);

    assert_eq!(engine.optimize_budgeted(512).unwrap(), (2, 2));
    assert_eq!(engine.optimize_backlog().raw_entries, 512);
    let profile = engine.profile();
    assert_eq!(profile.optimize_budgeted_count, 1);
    assert_eq!(profile.optimize_budget_entries, 512);
    assert_eq!(profile.optimize_budget_limited_count, 1);
    assert_eq!(profile.optimize_raw_entries, 512);

    assert_eq!(engine.optimize_budgeted(512).unwrap(), (2, 2));
    assert_eq!(engine.optimize_backlog().raw_entries, 0);
    assert_eq!(engine.query(&full_range_query()).unwrap().len(), 1_024);
    assert!(engine
        .optimize_budgeted(0)
        .unwrap_err()
        .contains("positive"));
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
    assert_eq!(
        msgs,
        ["flushed-10", "buffered-20", "flushed-30", "buffered-40"]
    );

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

#[test]
fn bounded_query_is_exact_for_overlaps_ties_buffer_and_both_orders() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&["service"])).unwrap();

    // Two overlapping persisted ranges with duplicate timestamps, followed by
    // a matching in-memory generation. Canonical tie order is block creation
    // order, row order, then buffered insertion order.
    for (ts, message, service) in [
        (10, "b0-10", "api"),
        (30, "b0-30-a", "api"),
        (30, "b0-30-b", "web"),
    ] {
        engine
            .push(entry(ts, 1, message, &[("service", service)]))
            .unwrap();
    }
    engine.flush().unwrap();
    for (ts, message, service) in [
        (5, "b1-05", "api"),
        (20, "b1-20", "web"),
        (30, "b1-30", "api"),
        (40, "b1-40", "api"),
    ] {
        engine
            .push(entry(ts, 1, message, &[("service", service)]))
            .unwrap();
    }
    engine.flush().unwrap();
    for (ts, message, service) in [
        (0, "buf-00", "api"),
        (30, "buf-30", "api"),
        (50, "buf-50", "web"),
    ] {
        engine
            .push(entry(ts, 1, message, &[("service", service)]))
            .unwrap();
    }

    let asc = [
        "buf-00", "b1-05", "b0-10", "b1-20", "b0-30-a", "b0-30-b", "b1-30", "buf-30", "b1-40",
        "buf-50",
    ];
    let desc = [
        "buf-50", "b1-40", "b0-30-a", "b0-30-b", "b1-30", "buf-30", "b1-20", "b0-10", "b1-05",
        "buf-00",
    ];
    for capacity in [0, 1, 4, 6, 10, 20, usize::MAX] {
        let got = engine
            .query_bounded(&full_range_query(), LogQueryOrder::Asc, capacity)
            .unwrap();
        assert_eq!(
            got.iter()
                .map(|entry| entry.message.as_str())
                .collect::<Vec<_>>(),
            asc[..capacity.min(asc.len())]
        );

        let got = engine
            .query_bounded(&full_range_query(), LogQueryOrder::Desc, capacity)
            .unwrap();
        assert_eq!(
            got.iter()
                .map(|entry| entry.message.as_str())
                .collect::<Vec<_>>(),
            desc[..capacity.min(desc.len())]
        );
    }

    // Sparse exact filters are applied before a row can occupy the bound.
    let api = LogQuery {
        metadata_eq: vec![("service".into(), "api".into())],
        ..full_range_query()
    };
    let got = engine.query_bounded(&api, LogQueryOrder::Desc, 4).unwrap();
    assert_eq!(
        got.iter()
            .map(|entry| entry.message.as_str())
            .collect::<Vec<_>>(),
        ["b1-40", "b0-30-a", "b1-30", "buf-30"]
    );
}

#[test]
fn bounded_query_stops_on_block_bounds_and_reports_bounded_work() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&[])).unwrap();
    for base in [0i64, 100, 200] {
        for i in 0..10 {
            engine
                .push(entry(base + i, 1, &format!("disk-{base}-{i}"), &[]))
                .unwrap();
        }
        engine.flush().unwrap();
    }
    engine.push(entry(1_000, 1, "buffer-1000", &[])).unwrap();
    engine.push(entry(1_001, 1, "buffer-1001", &[])).unwrap();

    let got = engine
        .query_bounded(&full_range_query(), LogQueryOrder::Desc, 2)
        .unwrap();
    assert_eq!(
        got.iter()
            .map(|entry| entry.message.as_str())
            .collect::<Vec<_>>(),
        ["buffer-1001", "buffer-1000"]
    );
    let profile = engine.profile();
    assert_eq!(profile.query_bounded_count, 1);
    assert_eq!(profile.query_bounded_requested_entries, 2);
    assert_eq!(profile.query_bounded_max_entries, 2);
    assert_eq!(profile.query_blocks_skipped_by_bound, 3);
    assert_eq!(profile.query_decoded_entries, 0);
    assert_eq!(profile.query_matched_entries, 2);
    assert_eq!(profile.query_returned_entries, 2);
}

#[test]
fn field_values_are_exact_deterministic_and_bounded_across_buffer_and_blocks() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&["host"])).unwrap();
    for (ts, level, host) in [(10, 3, "web-c"), (20, 2, "web-warning"), (30, 3, "web-a")] {
        engine
            .push(entry(ts, level, host, &[("host", host), ("env", "prod")]))
            .unwrap();
    }
    engine.flush().unwrap();
    engine
        .push(entry(40, 3, "web-b", &[("host", "web-b"), ("env", "prod")]))
        .unwrap();
    engine
        .push(entry(
            50,
            3,
            "dev-only",
            &[("host", "web-0"), ("env", "dev")],
        ))
        .unwrap();

    let query = LogQuery {
        level: Some(3),
        severity: Some("error".into()),
        metadata_eq: vec![("env".into(), "prod".into())],
        ..full_range_query()
    };
    assert_eq!(
        engine.field_values(&query, "host", 2).unwrap(),
        ["web-a", "web-b"]
    );
    assert_eq!(
        engine.field_values(&query, "host", 10).unwrap(),
        ["web-a", "web-b", "web-c"]
    );
    assert!(engine.field_values(&query, "host", 0).unwrap().is_empty());
    assert!(engine.field_values(&query, "", 1).is_err());
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
        fn replace_terms(&self, loc: &BlockLoc, terms: &[String]) -> Result<(), String> {
            self.0.replace_terms(loc, terms)
        }
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
    let engine2 = BlockEngine::new(Box::new(SharedStore(store)), config(&["service"])).unwrap();
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
        vec![
            ("a".to_string(), "2".to_string()),
            ("z".to_string(), "3".to_string())
        ]
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

#[test]
fn savepoint_rollback_is_repeatable_and_unwinds_nested_frames() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&[])).unwrap();
    engine.txn_begin();
    engine.push(entry(1, 1, "outer", &[])).unwrap();
    engine.txn_savepoint(0);
    engine.push(entry(2, 1, "inner-1", &[])).unwrap();
    engine.txn_savepoint(1);
    engine.push(entry(3, 1, "inner-2", &[])).unwrap();

    engine.txn_rollback_to(0);
    assert_eq!(engine.buffered_count(), 1);
    engine.push(entry(4, 1, "inner-again", &[])).unwrap();
    engine.txn_rollback_to(0);
    engine.txn_release(0);
    engine.txn_commit();

    let got = engine.query(&full_range_query()).unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].message, "outer");
}

#[test]
fn released_flush_frame_still_rolls_back_with_outer_transaction() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&[])).unwrap();
    engine.push(entry(1, 1, "pre", &[])).unwrap();
    engine.txn_begin();
    engine.push(entry(2, 1, "outer", &[])).unwrap();
    engine.txn_savepoint(0);
    engine.flush().unwrap();
    engine.txn_release(0);
    engine.txn_rollback();

    assert_eq!(engine.stats(), (0, 0, 1));
}

struct CancellingQueryStore {
    inner: MemBlockStore,
    cancel_on_read: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
}

impl BlockStore for CancellingQueryStore {
    fn query_snapshot_keeps_locations_readable(&self) -> bool {
        true
    }

    fn check_cancelled(&self) -> Result<(), String> {
        if self.cancelled.load(Ordering::Acquire) {
            Err("test log query cancelled".into())
        } else {
            Ok(())
        }
    }

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
        let bytes = self.inner.read_block(loc)?;
        if self.cancel_on_read.load(Ordering::Acquire) {
            self.cancelled.store(true, Ordering::Release);
        }
        Ok(bytes)
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
fn query_work_limits_bound_decode_and_cancellation_leaves_the_engine_reusable() {
    let cancel_on_read = Arc::new(AtomicBool::new(false));
    let cancelled = Arc::new(AtomicBool::new(false));
    let engine = BlockEngine::new(
        Box::new(CancellingQueryStore {
            inner: MemBlockStore::new(),
            cancel_on_read: Arc::clone(&cancel_on_read),
            cancelled: Arc::clone(&cancelled),
        }),
        config(&["service"]),
    )
    .unwrap();
    for ts in 0..10 {
        engine
            .push(entry(ts, 1, "work", &[("service", "api")]))
            .unwrap();
    }
    engine.flush().unwrap();

    let query = LogQuery {
        message_contains: Some("work".into()),
        ..full_range_query()
    };
    let error = engine
        .query_ordered_with_work_limit_after_snapshot(
            &query,
            LogQueryOrder::Asc,
            Some(1),
            Some(9),
            || {},
        )
        .unwrap_err();
    assert_eq!(error, "log query exceeded max_work_entries=9");

    engine
        .push(entry(10, 1, "buffered", &[("service", "api")]))
        .unwrap();
    engine
        .push(entry(11, 1, "buffered", &[("service", "api")]))
        .unwrap();
    let mut snapshot_completed = false;
    let error = engine
        .query_ordered_with_work_limit_after_snapshot(
            &query,
            LogQueryOrder::Asc,
            Some(1),
            Some(1),
            || snapshot_completed = true,
        )
        .unwrap_err();
    assert_eq!(error, "log query exceeded max_work_entries=1");
    assert!(
        !snapshot_completed,
        "live-buffer work must fail before filtering/cloning completes"
    );
    engine.flush().unwrap();

    cancel_on_read.store(true, Ordering::Release);
    let error = engine
        .query_ordered_with_work_limit_after_snapshot(
            &query,
            LogQueryOrder::Asc,
            Some(12),
            Some(12),
            || {},
        )
        .unwrap_err();
    assert_eq!(error, "test log query cancelled");

    cancel_on_read.store(false, Ordering::Release);
    cancelled.store(false, Ordering::Release);
    assert_eq!(
        engine
            .query_ordered_with_work_limit_after_snapshot(
                &query,
                LogQueryOrder::Asc,
                Some(12),
                Some(12),
                || {},
            )
            .unwrap()
            .len(),
        10
    );
}

#[test]
fn request_query_report_is_exact_and_does_not_require_profile_deltas() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&["service"])).unwrap();
    for ts in 0..4 {
        let service = if ts == 1 { "api" } else { "worker" };
        engine
            .push(entry(ts, 1, "persisted", &[("service", service)]))
            .unwrap();
    }
    engine.flush().unwrap();
    engine
        .push(entry(4, 1, "buffered", &[("service", "api")]))
        .unwrap();
    engine
        .push(entry(5, 1, "buffered", &[("service", "worker")]))
        .unwrap();

    let query = LogQuery {
        metadata_eq: vec![("service".into(), "api".into())],
        ..full_range_query()
    };
    let (rows, report) = engine
        .query_ordered_with_work_limit_report_after_snapshot(
            &query,
            LogQueryOrder::Asc,
            None,
            Some(6),
            || {},
        )
        .unwrap();

    assert_eq!(rows.iter().map(|row| row.ts).collect::<Vec<_>>(), [1, 4]);
    assert_eq!(report.candidate_blocks, 1);
    assert_eq!(report.processed_blocks, 1);
    assert_eq!(report.blocks_skipped_by_bound, 0);
    assert_eq!(report.buffered_entries_processed, 2);
    assert_eq!(report.decoded_entries, 4);
    assert_eq!(report.processed_entries, 6);
    assert_eq!(report.matched_entries, 2);
    assert_eq!(report.returned_entries, 2);
    assert_eq!(report.values_read, 18);
    assert_eq!(report.timestamps_read, 6);
    assert!(report.payload_bytes_read > 0);
    assert_eq!(report.payload_bytes_read, report.snapshot_payload_bytes);
    assert!(!report.stable_location_snapshot);
}

// ---------------------------------------------------------------------------
// Auto-optimize: compression must happen for hosts that only ever flush.
// The embedded Elixir engines send 'flush' on a heartbeat and never
// 'optimize' — a store that stays raw until someone remembers to schedule
// maintenance is the bug these tests pin down.
// ---------------------------------------------------------------------------

fn auto_config(interval: usize, budget: usize) -> BlockEngineConfig {
    BlockEngineConfig {
        auto_optimize_interval_flushes: interval,
        auto_optimize_budget_entries: budget,
        ..BlockEngineConfig::default()
    }
}

#[test]
fn auto_optimize_compresses_after_interval_flushes_without_manual_call() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), auto_config(3, 32_768)).unwrap();
    for i in 0..200i64 {
        engine
            .push(entry(1_000 + i, (i % 4) as u8, &format!("m {i}"), &[]))
            .unwrap();
    }
    engine.flush().unwrap();
    let (_, raw_after_first, _) = engine.stats();
    assert!(raw_after_first > 0, "first flush leaves raw blocks");

    // Empty heartbeat flushes — the only signal an embedded host sends.
    engine.flush().unwrap();
    engine.flush().unwrap();

    let (blocks, raw_blocks, _) = engine.stats();
    assert!(blocks > 0);
    assert_eq!(
        raw_blocks, 0,
        "interval-th flush call must compress the raw backlog by itself"
    );
}

#[test]
fn auto_optimize_triggers_immediately_when_raw_backlog_reaches_budget() {
    // Interval far out of reach: only the budget-sized backlog can trigger.
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), auto_config(1_000, 100)).unwrap();
    for i in 0..150i64 {
        engine
            .push(entry(1_000 + i, 1, &format!("m {i}"), &[]))
            .unwrap();
    }
    engine.flush().unwrap();
    let (_, raw_blocks, _) = engine.stats();
    assert_eq!(
        raw_blocks, 0,
        "a raw backlog at the budget compresses on the same flush, not interval-later"
    );
}

#[test]
fn auto_optimize_zero_interval_disables() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), auto_config(0, 100)).unwrap();
    for i in 0..200i64 {
        engine
            .push(entry(1_000 + i, 1, &format!("m {i}"), &[]))
            .unwrap();
    }
    for _ in 0..40 {
        engine.flush().unwrap();
    }
    let (blocks, raw_blocks, _) = engine.stats();
    assert_eq!(
        raw_blocks, blocks,
        "disabled auto-optimize never compresses"
    );
    assert!(raw_blocks > 0);
}

#[test]
fn compression_totals_persist_across_reopen_and_credit_merges() {
    let shared = Arc::new(MemBlockStore::new());
    let engine = BlockEngine::new(Box::new(SharedStore(Arc::clone(&shared))), config(&[])).unwrap();
    for i in 0..100i64 {
        engine
            .push(entry(1_000 + i, 1, &format!("m {i}"), &[]))
            .unwrap();
    }
    engine.flush().unwrap();
    engine.optimize().unwrap();
    let (in1, out1) = engine.load_compression_totals().unwrap();
    assert!(
        in1 > 0 && out1 > 0,
        "optimize records raw->compressed bytes"
    );

    // A no-op optimize (one lone small block, nothing raw) moves nothing.
    engine.optimize().unwrap();
    assert_eq!(engine.load_compression_totals().unwrap(), (in1, out1));

    // Second batch compresses raw AND may merge the two small compressed
    // blocks: the input side grows by exactly the raw bytes; the output
    // side is credited for the merge shrink rather than freezing at the
    // tiny-block first-pass footprint.
    for i in 0..100i64 {
        engine
            .push(entry(2_000 + i, 1, &format!("n {i}"), &[]))
            .unwrap();
    }
    engine.flush().unwrap();
    engine.optimize().unwrap();
    let (in2, out2) = engine.load_compression_totals().unwrap();
    assert!(in2 > in1, "input side accumulates raw bytes only");

    // Force the merge tier (both compressed blocks are far below the
    // target): output must now reflect the merged footprint — the sum of
    // current compressed block bytes, not the pre-merge total.
    engine.optimize().unwrap();
    let (in3, out3) = engine.load_compression_totals().unwrap();
    assert_eq!(in3, in2, "merges never touch the input side");
    let on_disk: u64 = shared
        .scan()
        .unwrap()
        .iter()
        .map(|(_, loc)| shared.read_block(loc).unwrap().len() as u64)
        .sum();
    assert_eq!(
        out3, on_disk,
        "output side tracks the current compressed footprint"
    );
    assert!(out3 <= out2, "merge shrink is credited, never penalized");

    // Durable: a fresh engine over the same store reads the totals back.
    drop(engine);
    let reopened =
        BlockEngine::new(Box::new(SharedStore(Arc::clone(&shared))), config(&[])).unwrap();
    assert_eq!(reopened.load_compression_totals().unwrap(), (in3, out3));
}

// ---------------------------------------------------------------------------
// CLP-dictionary pruning (issue #2): message_contains on codec-8 blocks
// proves absence from the template/variable dictionaries without decoding,
// and pruned blocks do not count against max_work_entries.
// ---------------------------------------------------------------------------

#[test]
fn clp_dictionary_pruning_skips_infeasible_blocks() {
    let engine = BlockEngine::new(Box::new(MemBlockStore::new()), config(&[])).unwrap();
    let rich_entry = |ts: i64, message: String| LogEntry {
        ts,
        level: 1,
        severity: Some("info".into()),
        message,
        metadata: vec![("service".into(), "dhcp".into())],
        metadata_json: Some("{\"service\":\"dhcp\"}".into()),
    };
    // Templated CMTS-style lines: codec 8 wins these blocks.
    for i in 0..2048i64 {
        engine
            .push(rich_entry(
                1_785_600_000_000_000 + i,
                format!(
                    "DHCP NAK - MAC:00:1a:2b:{:02x} lease 10.0.{}.{} expired after {}s",
                    i % 256,
                    i % 256,
                    (i * 7) % 256,
                    30 + i % 900
                ),
            ))
            .unwrap();
    }
    engine.flush().unwrap();
    engine.optimize().unwrap();
    assert!(engine.stats().0 >= 1);

    // Absent needle: every block is pruned from the dictionaries alone —
    // zero entries decoded, zero rows returned.
    let before = engine.profile();
    let absent = LogQuery {
        message_contains: Some("PROVISIONING FAILURE".into()),
        ..full_range_query()
    };
    let got = engine.query_after_snapshot(&absent, || {}).unwrap();
    assert!(got.is_empty());
    let after = engine.profile();
    assert!(
        after.query_clp_pruned_blocks > before.query_clp_pruned_blocks,
        "expected CLP pruning to fire"
    );
    assert_eq!(
        after.query_decoded_entries, before.query_decoded_entries,
        "absent needle must decode nothing"
    );

    // The issue #2 acceptance shape: an absent needle under a work budget
    // far smaller than the store must SUCCEED, because pruned blocks are
    // never decoded and therefore never charged.
    let got = engine
        .query_ordered_with_work_limit_after_snapshot(
            &absent,
            LogQueryOrder::Desc,
            Some(10),
            Some(100),
            || {},
        )
        .unwrap();
    assert!(got.is_empty());

    // Native count agrees and prunes too.
    let count_before = engine.profile();
    assert_eq!(engine.count(&absent).unwrap(), 0);
    let count_after = engine.profile();
    assert!(count_after.query_clp_pruned_blocks > count_before.query_clp_pruned_blocks);

    // Present needles still return exact results, any case.
    let present = LogQuery {
        message_contains: Some("lease 10.0.3.21 EXPIRED".into()),
        ..full_range_query()
    };
    // i ≡ 3 (mod 256) renders "10.0.3.21": 8 hits across 2048 entries.
    let rows = engine.query_after_snapshot(&present, || {}).unwrap();
    assert_eq!(rows.len(), 8);
    assert_eq!(engine.count(&present).unwrap(), 8);
    let broad = LogQuery {
        message_contains: Some("dhcp nak".into()),
        ..full_range_query()
    };
    assert_eq!(engine.count(&broad).unwrap(), 2048);
}
