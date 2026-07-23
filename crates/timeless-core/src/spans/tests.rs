//! SpanBlockEngine unit tests (Session 6 acceptance list):
//!   - span codec round-trip exactness (negative ts, missing parents,
//!     unicode attributes) + validation edges
//!   - STATUS-partitioned flush: status-pure blocks, single status: term
//!   - trace query reads ONLY blocks containing the trace (read-count
//!     proof — the hero pushdown, measured not assumed)
//!   - optimize merges within a status partition, never across
//!   - recovery re-derives partitions from the status: posting lists
//!   - prune drops blocks AND their trace-index rows (never-dangle)
//!
//! One instrumented wrapper store serves every test (the logs test file
//! grew four near-identical wrappers; learning applied).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::codec::{
    decode_span_block, encode_span_block, CODEC_COLUMNAR, CODEC_COLUMNAR_V2, CODEC_RAW, CODEC_ZSTD,
};
use crate::blocks::codec::{PAIRS_LEGACY, PAIRS_SHREDDED, SHRED_MAX_KEYS};
use super::engine::{SpanBlockEngine, SpanEngineConfig, SpanQuery};
use super::mem::MemSpanStore;
use super::{
    kind_from_name, status_from_name, BlockLoc, BlockMeta, EncodedSpanBlock, SpanBlockStore,
    SpanEntry,
};

/// Deterministic 16-byte trace id from a small seed (t repeated).
fn tid(t: u8) -> [u8; 16] {
    [t; 16]
}

fn sid(s: u8) -> [u8; 8] {
    [s; 8]
}

fn span(
    trace: u8,
    span_n: u8,
    parent: Option<u8>,
    name: &str,
    service: &str,
    kind: u8,
    status: u8,
    start_ts: i64,
    attrs: &[(&str, &str)],
) -> SpanEntry {
    SpanEntry {
        trace_id: tid(trace),
        span_id: sid(span_n),
        parent_span_id: parent.map(sid),
        name: name.to_owned(),
        service: service.to_owned(),
        kind,
        status,
        start_ts,
        duration_ns: 1_000 + start_ts % 997,
        attributes: attrs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
    }
}

fn full_range_query() -> SpanQuery {
    SpanQuery {
        ts_min: i64::MIN + 1,
        ts_max: i64::MAX - 1,
        trace_id: None,
        service: None,
        kind: None,
        status: None,
        name: None,
    }
}

// ---------------------------------------------------------------------------
// Instrumented store: wraps a shared MemSpanStore, counts payload reads
// and records the term sets of everything persisted. One wrapper for
// every test that needs shared state, read counts, or term capture.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct SpyStore {
    inner: Arc<MemSpanStore>,
    reads: Arc<AtomicUsize>,
    /// Term sets from put_blocks (flush) and replace_blocks (optimize).
    put_terms: Arc<Mutex<Vec<Vec<String>>>>,
    replace_terms: Arc<Mutex<Vec<Vec<String>>>>,
}

impl SpyStore {
    fn new() -> Self {
        SpyStore {
            inner: Arc::new(MemSpanStore::new()),
            reads: Arc::new(AtomicUsize::new(0)),
            put_terms: Arc::new(Mutex::new(Vec::new())),
            replace_terms: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl SpanBlockStore for SpyStore {
    fn put_blocks(&self, blocks: &[EncodedSpanBlock]) -> Result<Vec<BlockLoc>, String> {
        let mut rec = self.put_terms.lock().unwrap();
        for b in blocks {
            rec.push(b.terms.clone());
        }
        drop(rec);
        self.inner.put_blocks(blocks)
    }
    fn replace_blocks(
        &self,
        add: &[EncodedSpanBlock],
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
    fn query_trace(&self, trace_id: &[u8; 16]) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        self.inner.query_trace(trace_id)
    }
    fn save_meta(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.inner.save_meta(key, value)
    }
    fn load_meta(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.inner.load_meta(key)
    }
}

fn status_terms_of(terms: &[String]) -> Vec<&String> {
    terms.iter().filter(|t| t.starts_with("status:")).collect()
}

// ---------------------------------------------------------------------------
// Codec
// ---------------------------------------------------------------------------

#[test]
fn span_codec_round_trips_all_codecs() {
    // Deliberately hostile: NEGATIVE start_ts (pre-epoch ns — legal,
    // the engine never assumes an epoch), a root span with no parent,
    // unicode attribute values, empty strings, negative-delta ordering
    // (entries NOT sorted by ts).
    let entries = vec![
        SpanEntry {
            trace_id: [0xAB; 16],
            span_id: [0x01; 8],
            parent_span_id: None,
            name: "GET /checkout".into(),
            service: "api".into(),
            kind: 1,
            status: 2,
            start_ts: 1_700_000_000_000_000_123,
            duration_ns: 52_000_000,
            attributes: vec![
                ("http.method".into(), "GET".into()),
                ("note".into(), "空 🚀 ünïcode".into()),
            ],
        },
        SpanEntry {
            trace_id: [0x00; 16],
            span_id: [0xFF; 8],
            parent_span_id: Some([0x01; 8]),
            name: "".into(), // empty name survives
            service: "db".into(),
            kind: 2,
            status: 0,
            start_ts: -42, // negative + out-of-order → negative delta path
            duration_ns: 0,
            attributes: vec![],
        },
        SpanEntry {
            trace_id: [0xAB; 16],
            span_id: [0x02; 8],
            parent_span_id: Some([0x01; 8]),
            name: "db.query".into(),
            service: "db".into(),
            kind: 3,
            status: 1,
            start_ts: 1_700_000_000_000_000_999,
            duration_ns: i64::MAX, // extreme duration must survive
            attributes: vec![("k".into(), "".into())],
        },
    ];
    // CODEC_ZSTD and CODEC_COLUMNAR stay in this loop FOREVER even
    // though optimize() no longer writes them: the legacy encoder
    // paths are retained, and this round-trip is the proof that
    // existing codec-2/4 span blocks remain decodable.
    for codec in [CODEC_RAW, CODEC_ZSTD, CODEC_COLUMNAR, CODEC_COLUMNAR_V2] {
        let (bytes, meta) = encode_span_block(&entries, codec, 7).unwrap();
        assert_eq!(meta.ts_min, -42);
        assert_eq!(meta.ts_max, 1_700_000_000_000_000_999);
        assert_eq!(meta.entry_count, 3);
        assert_eq!(meta.codec, codec);
        let back = decode_span_block(&bytes).unwrap();
        assert_eq!(back, entries, "codec {codec} round-trip");
    }
}

// ---------------------------------------------------------------------------
// Codec-5 attribute shredding: the spans twin of the logs hostile
// tests (the shredding code is SHARED — blocks/codec.rs — so this is
// mostly proving the spans container wires it up correctly).
// ---------------------------------------------------------------------------

/// Strategy byte of a codec-5 span block's attributes column (10th
/// column: header has 10 u32 lengths at offset 22, columns from 62).
fn attributes_strategy_byte(bytes: &[u8]) -> u8 {
    let len = |i: usize| {
        u32::from_le_bytes(bytes[22 + i * 4..26 + i * 4].try_into().unwrap()) as usize
    };
    bytes[62 + (0..9).map(len).sum::<usize>()]
}

fn rt_spans_v2(entries: &[SpanEntry], expect_strategy: u8, label: &str) {
    let (bytes, meta) = encode_span_block(entries, CODEC_COLUMNAR_V2, 7).unwrap();
    assert_eq!(meta.codec, CODEC_COLUMNAR_V2);
    assert_eq!(attributes_strategy_byte(&bytes), expect_strategy, "{label}: strategy byte");
    let back = decode_span_block(&bytes).unwrap();
    assert_eq!(&back, entries, "{label}: round-trip");
}

#[test]
fn span_codec5_shreds_hostile_attribute_shapes_exactly() {
    // Disjoint key sets + an empty-attribute span + unicode.
    rt_spans_v2(
        &[
            span(1, 1, None, "a", "api", 1, 1, 100, &[("alpha", "1")]),
            span(1, 2, Some(1), "b", "db", 2, 1, 200, &[("ベータ", "値🔥")]),
            span(2, 3, None, "c", "cache", 0, 1, 300, &[]),
        ],
        PAIRS_SHREDDED,
        "disjoint + empty + unicode",
    );

    // All spans empty attributes; single span; all same pairs.
    rt_spans_v2(&[span(1, 1, None, "a", "api", 1, 1, 100, &[])], PAIRS_SHREDDED, "single, empty");
    let same: Vec<SpanEntry> = (0..300)
        .map(|i| {
            span(1, i as u8, None, "op", "api", 1, 1, 1000 + i,
                 &[("http.method", "GET"), ("http.status", "200")])
        })
        .collect();
    rt_spans_v2(&same, PAIRS_SHREDDED, "all same pairs");
}

#[test]
fn span_codec5_key_explosion_falls_back_to_legacy() {
    // > SHRED_MAX_KEYS distinct attribute keys → LEGACY bytes verbatim
    // (see the cap rationale in blocks/codec.rs), still exact.
    let entries: Vec<SpanEntry> = (0..(SHRED_MAX_KEYS as i64 + 1))
        .map(|i| {
            span(1, i as u8, None, "op", "api", 1, 1, i,
                 &[(&format!("key-{i:03}") as &str, "v")])
        })
        .collect();
    rt_spans_v2(&entries, PAIRS_LEGACY, "key explosion");
}

#[test]
fn span_codec_rejects_garbage() {
    let entries = vec![span(1, 1, None, "op", "svc", 1, 1, 100, &[("a", "b")])];
    let (bytes, _) = encode_span_block(&entries, CODEC_ZSTD, 7).unwrap();
    // Truncation anywhere must be an error, never a panic.
    for cut in [0, 1, 30, 61, bytes.len() - 1] {
        assert!(decode_span_block(&bytes[..cut]).is_err(), "cut at {cut}");
    }
    // Bad kind / status at encode time.
    assert!(encode_span_block(
        &[span(1, 1, None, "op", "svc", 5, 1, 100, &[])],
        CODEC_RAW,
        7
    )
    .is_err());
    assert!(encode_span_block(
        &[span(1, 1, None, "op", "svc", 1, 3, 100, &[])],
        CODEC_RAW,
        7
    )
    .is_err());
    // Empty blocks are refused.
    assert!(encode_span_block(&[], CODEC_RAW, 7).is_err());
}

#[test]
fn optimize_writes_codec_5_blocks_that_decode_exactly() {
    // The traces twin of the logs test with the same name: after
    // optimize, every persisted block must carry codec byte 5
    // (CODEC_COLUMNAR_V2) — in the store metadata AND in the payload
    // itself — and decode back to exactly the pushed spans.
    let shared = Arc::new(MemSpanStore::new());
    let store = SpyStore {
        inner: Arc::clone(&shared),
        reads: Arc::new(AtomicUsize::new(0)),
        put_terms: Arc::new(Mutex::new(Vec::new())),
        replace_terms: Arc::new(Mutex::new(Vec::new())),
    };
    let engine = SpanBlockEngine::new(Box::new(store), SpanEngineConfig::default()).unwrap();

    let mut expect = Vec::new();
    for i in 0..200u8 {
        let e = span(
            i % 7,
            i,
            (i % 5 != 0).then_some(i / 2),
            ["GET /x", "db.query", "cache.get"][i as usize % 3],
            ["api", "db", "cache"][i as usize % 3],
            (i % 5) as u8,
            (i % 3) as u8,
            10_000 + i as i64,
            &[("http.method", "GET")],
        );
        expect.push(e.clone());
        engine.push(e).unwrap();
    }
    engine.flush().unwrap();
    engine.optimize().unwrap();

    let mut decoded = Vec::new();
    let scanned = shared.scan().unwrap();
    assert!(!scanned.is_empty());
    for (meta, loc) in scanned {
        assert_eq!(meta.codec, CODEC_COLUMNAR_V2, "store meta codec byte");
        let bytes = shared.read_block(&loc).unwrap();
        assert_eq!(bytes[1], CODEC_COLUMNAR_V2, "payload codec byte");
        decoded.extend(decode_span_block(&bytes).unwrap());
    }
    decoded.sort_by_key(|e| (e.start_ts, e.span_id));
    expect.sort_by_key(|e| (e.start_ts, e.span_id));
    assert_eq!(decoded, expect, "codec-5 optimize output round-trips");
}

#[test]
fn kind_and_status_names_are_strict() {
    assert_eq!(kind_from_name("internal").unwrap(), 0);
    assert_eq!(kind_from_name("server").unwrap(), 1);
    assert_eq!(kind_from_name("client").unwrap(), 2);
    assert_eq!(kind_from_name("producer").unwrap(), 3);
    assert_eq!(kind_from_name("consumer").unwrap(), 4);
    assert!(kind_from_name("SERVER").is_err()); // no case folding
    assert!(kind_from_name("span").is_err());
    assert_eq!(status_from_name("unset").unwrap(), 0);
    assert_eq!(status_from_name("ok").unwrap(), 1);
    assert_eq!(status_from_name("error").unwrap(), 2);
    assert!(status_from_name("OK").is_err());
    assert!(status_from_name("failed").is_err());
}

// ---------------------------------------------------------------------------
// Status-partitioned flush + round-trip exactness
// ---------------------------------------------------------------------------

#[test]
fn flush_writes_status_pure_blocks_with_single_status_term() {
    let store = SpyStore::new();
    let put_terms = Arc::clone(&store.put_terms);
    let engine =
        SpanBlockEngine::new(Box::new(store), SpanEngineConfig::default()).unwrap();

    // Interleave all three statuses in one buffer — an unpartitioned
    // flush would write ONE block carrying all three status: terms.
    let mut expect = Vec::new();
    for i in 0..30i64 {
        let e = span(
            (i % 5) as u8,
            i as u8,
            if i % 5 == 0 { None } else { Some((i - 1) as u8) },
            "op",
            "api",
            (i % 5) as u8,
            (i % 3) as u8,
            1_000 + i,
            &[("http.method", "GET")],
        );
        expect.push(e.clone());
        engine.push(e).unwrap();
    }
    // Queryable BEFORE flush (buffer path)...
    assert_eq!(engine.query(&full_range_query()).unwrap(), expect);
    assert_eq!(engine.flush().unwrap(), 30);
    // ...one block per status present, each with EXACTLY one status: term.
    assert_eq!(engine.stats().0, 3, "one raw block per status present");
    let recorded = put_terms.lock().unwrap();
    assert_eq!(recorded.len(), 3);
    for terms in recorded.iter() {
        assert_eq!(
            status_terms_of(terms).len(),
            1,
            "status-pure block must emit one status: term, got {terms:?}"
        );
        // Non-status terms present too (service/kind/name indexing).
        assert!(terms.iter().any(|t| t == "service:api"));
        assert!(terms.iter().any(|t| t == "name:op"));
    }
    drop(recorded);
    // The partitioned layout is invisible to queries: exact round-trip.
    assert_eq!(engine.query(&full_range_query()).unwrap(), expect);

    // ...and identical after optimize (zstd path, per-partition).
    let (removed, written) = engine.optimize().unwrap();
    assert_eq!((removed, written), (3, 3));
    assert_eq!(engine.query(&full_range_query()).unwrap(), expect);

    // Filtered queries are exact.
    let q = SpanQuery {
        status: Some(2),
        service: Some("api".into()),
        ..full_range_query()
    };
    let got = engine.query(&q).unwrap();
    let want: Vec<SpanEntry> = expect.iter().filter(|e| e.status == 2).cloned().collect();
    assert!(!want.is_empty());
    assert_eq!(got, want);
}

// ---------------------------------------------------------------------------
// Trace index: the read-count proof
// ---------------------------------------------------------------------------

#[test]
fn trace_query_reads_only_blocks_containing_the_trace() {
    let store = SpyStore::new();
    let reads = Arc::clone(&store.reads);
    let engine =
        SpanBlockEngine::new(Box::new(store), SpanEngineConfig::default()).unwrap();

    // Three flushes → three (ok-pure) blocks, each holding different
    // traces; trace 7 lives ONLY in the middle block.
    for (base, traces) in [(1_000i64, [1u8, 2]), (2_000, [7, 8]), (3_000, [3, 4])] {
        for (j, t) in traces.iter().enumerate() {
            for s in 0..5u8 {
                engine
                    .push(span(
                        *t,
                        s,
                        (s > 0).then(|| s - 1),
                        "op",
                        "api",
                        1,
                        1,
                        base + (j as i64) * 10 + s as i64,
                        &[],
                    ))
                    .unwrap();
            }
        }
        engine.flush().unwrap();
    }
    assert_eq!(engine.stats().0, 3);

    reads.store(0, Ordering::SeqCst);
    let q = SpanQuery {
        trace_id: Some(tid(7)),
        ..full_range_query()
    };
    let got = engine.query(&q).unwrap();
    assert_eq!(got.len(), 5);
    assert!(got.iter().all(|e| e.trace_id == tid(7)));
    // THE assertion: only the one block containing trace 7 was read;
    // the trace index skipped the other two without touching payloads.
    assert_eq!(reads.load(Ordering::SeqCst), 1);

    // Unknown trace: zero reads, zero rows (index miss, no scan).
    reads.store(0, Ordering::SeqCst);
    let q = SpanQuery {
        trace_id: Some(tid(99)),
        ..full_range_query()
    };
    assert_eq!(engine.query(&q).unwrap().len(), 0);
    assert_eq!(reads.load(Ordering::SeqCst), 0);

    // Buffered spans of a trace surface too (queryable-before-flush).
    engine
        .push(span(7, 200, Some(0), "late", "api", 1, 1, 9_000, &[]))
        .unwrap();
    let q = SpanQuery {
        trace_id: Some(tid(7)),
        ..full_range_query()
    };
    assert_eq!(engine.query(&q).unwrap().len(), 6);
}

// ---------------------------------------------------------------------------
// Optimize: merge within status partitions only, trace rows follow
// ---------------------------------------------------------------------------

#[test]
fn optimize_merges_within_status_partition_only() {
    let store = SpyStore::new();
    let inner = Arc::clone(&store.inner);
    let replace_terms = Arc::clone(&store.replace_terms);
    let engine =
        SpanBlockEngine::new(Box::new(store.clone()), SpanEngineConfig::default()).unwrap();

    // Three flushes, each with ok AND error spans → six pure raw
    // blocks whose time ranges overlap EXACTLY across partitions — a
    // status-blind merge would combine them.
    for (f, base) in [(0u8, 1_000i64), (1, 2_000), (2, 3_000)] {
        for i in 0..10i64 {
            engine
                .push(span(f * 2, i as u8, None, "op", "api", 1, 1, base + i, &[]))
                .unwrap();
            engine
                .push(span(f * 2 + 1, i as u8, None, "op", "api", 1, 2, base + i, &[]))
                .unwrap();
        }
        engine.flush().unwrap();
    }
    assert_eq!(engine.stats().0, 6, "3 flushes x 2 statuses = 6 pure blocks");

    // Partitioned optimize: 3 ok → 1, 3 error → 1, never across.
    let (removed, written) = engine.optimize().unwrap();
    assert_eq!((removed, written), (6, 2));
    for terms in replace_terms.lock().unwrap().iter() {
        assert_eq!(
            status_terms_of(terms).len(),
            1,
            "merged block crossed status partitions: {terms:?}"
        );
    }
    assert_eq!(engine.query(&full_range_query()).unwrap().len(), 60);

    // The trace index followed the swap: exactly one (trace, block)
    // row per trace (each trace now lives in exactly one merged
    // block), and every trace still resolves.
    assert_eq!(inner.trace_index_rows(), 6);
    for t in 0..6u8 {
        let q = SpanQuery {
            trace_id: Some(tid(t)),
            ..full_range_query()
        };
        assert_eq!(engine.query(&q).unwrap().len(), 10, "trace {t} lost spans");
    }
    drop(engine);

    // Recovery proof: a fresh engine re-derives partitions from the
    // status: posting lists. Misclassifying the two pure blocks as
    // mixed would put them in one bucket and a second optimize would
    // merge them (2,1); correct derivation leaves both alone (0,0).
    let engine2 =
        SpanBlockEngine::new(Box::new(store), SpanEngineConfig::default()).unwrap();
    assert_eq!(
        engine2.optimize().unwrap(),
        (0, 0),
        "recovered partitions must keep ok/error blocks apart"
    );
}

#[test]
fn merge_respects_ts_span_cap() {
    // Cap of 100 ts units; three small raw blocks at ts ~0, ~50,
    // ~1000. Blocks 1+2 fit one 100-unit span; block 3 must not join.
    let cfg = SpanEngineConfig {
        merge_max_ts_span: 100,
        merge_target_entries: 1_000_000,
        ..SpanEngineConfig::default()
    };
    let engine = SpanBlockEngine::new(Box::new(MemSpanStore::new()), cfg).unwrap();
    for base in [0i64, 50, 1_000] {
        for i in 0..10 {
            engine
                .push(span(1, i as u8, None, "op", "api", 1, 1, base + i, &[]))
                .unwrap();
        }
        engine.flush().unwrap();
    }
    let (removed, written) = engine.optimize().unwrap();
    assert_eq!(removed, 3);
    assert_eq!(written, 2, "cap must split the merge into two blocks");
    assert_eq!(engine.query(&full_range_query()).unwrap().len(), 30);
}

// ---------------------------------------------------------------------------
// Recovery + prune
// ---------------------------------------------------------------------------

#[test]
fn recovery_rebuilds_index_and_trace_lookups() {
    let store = SpyStore::new();
    let engine =
        SpanBlockEngine::new(Box::new(store.clone()), SpanEngineConfig::default()).unwrap();
    for i in 0..20i64 {
        engine
            .push(span(
                (i % 4) as u8,
                i as u8,
                None,
                "op",
                "api",
                1,
                if i % 4 == 3 { 2 } else { 1 },
                1_000 + i,
                &[("http.status", "200")],
            ))
            .unwrap();
    }
    engine.flush().unwrap();
    engine.optimize().unwrap();
    let want = engine.query(&full_range_query()).unwrap();
    drop(engine);

    // "Reopen": fresh engine over the same store sees everything, and
    // the trace + term paths work from recovered state alone.
    let engine2 =
        SpanBlockEngine::new(Box::new(store), SpanEngineConfig::default()).unwrap();
    assert_eq!(engine2.query(&full_range_query()).unwrap(), want);
    assert_eq!(engine2.stats().0, 2, "ok + error partitions, one block each");
    let q = SpanQuery {
        trace_id: Some(tid(3)),
        ..full_range_query()
    };
    let got = engine2.query(&q).unwrap();
    assert_eq!(got.len(), 5);
    assert!(got.iter().all(|e| e.status == 2));
}

#[test]
fn prune_deletes_blocks_buffer_and_trace_rows() {
    let store = SpyStore::new();
    let inner = Arc::clone(&store.inner);
    let engine =
        SpanBlockEngine::new(Box::new(store), SpanEngineConfig::default()).unwrap();
    for i in 0..10 {
        engine
            .push(span(1, i as u8, None, "old", "api", 1, 1, 1_000 + i as i64, &[]))
            .unwrap();
    }
    engine.flush().unwrap();
    for i in 0..10 {
        engine
            .push(span(2, i as u8, None, "new", "api", 1, 1, 9_000 + i as i64, &[]))
            .unwrap();
    }
    engine.flush().unwrap();
    engine.push(span(3, 0, None, "old-buffered", "api", 1, 1, 500, &[])).unwrap();
    engine.push(span(4, 0, None, "new-buffered", "api", 1, 1, 9_500, &[])).unwrap();
    assert_eq!(inner.trace_index_rows(), 2);

    assert_eq!(engine.prune(5_000).unwrap(), 1); // one whole block gone
    let got = engine.query(&full_range_query()).unwrap();
    assert_eq!(got.len(), 11);
    assert!(got.iter().all(|e| e.start_ts >= 5_000));
    // Never-dangle: trace 1's index row died WITH its block.
    assert_eq!(inner.trace_index_rows(), 1);
    let q = SpanQuery {
        trace_id: Some(tid(1)),
        ..full_range_query()
    };
    assert_eq!(engine.query(&q).unwrap().len(), 0);
}

#[test]
fn push_validates_and_canonicalizes_attributes() {
    let engine =
        SpanBlockEngine::new(Box::new(MemSpanStore::new()), SpanEngineConfig::default()).unwrap();
    assert!(engine.push(span(1, 1, None, "op", "s", 5, 1, 1, &[])).is_err());
    assert!(engine.push(span(1, 1, None, "op", "s", 1, 3, 1, &[])).is_err());

    // Unsorted + duplicate keys: sorted, last duplicate wins.
    engine
        .push(span(1, 1, None, "op", "s", 1, 1, 1, &[("z", "1"), ("a", "2"), ("z", "3")]))
        .unwrap();
    let got = engine.query(&full_range_query()).unwrap();
    assert_eq!(
        got[0].attributes,
        vec![("a".to_string(), "2".to_string()), ("z".to_string(), "3".to_string())]
    );
}
