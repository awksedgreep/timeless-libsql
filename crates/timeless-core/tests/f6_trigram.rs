//! F6: trigram block pruning must be SOUND — the pruned query returns
//! exactly what the unpruned query returns, for hostile messages and
//! patterns — and it must actually prune (read-count proof).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use timeless_core::blocks::{BlockLoc, BlockMeta, EncodedBlock};
use timeless_core::{
    BlockEngine, BlockEngineConfig, BlockStore, LogEntry, LogQuery, MemBlockStore,
};

/// Counts read_block calls — the pruning proof.
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
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read_block(loc)
    }
    fn delete_blocks(&self, locs: &[BlockLoc]) -> Vec<String> {
        self.inner.delete_blocks(locs)
    }
    fn scan(&self) -> Result<Vec<(BlockMeta, BlockLoc)>, String> {
        self.inner.scan()
    }
    fn save_meta(&self, key: &str, value: &[u8]) -> Result<(), String> {
        self.inner.save_meta(key, value)
    }
    fn load_meta(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.inner.load_meta(key)
    }
    fn query_terms(
        &self,
        terms: &[String],
        ts_min: i64,
        ts_max: i64,
    ) -> Result<Vec<(BlockLoc, BlockMeta)>, String> {
        self.inner.query_terms(terms, ts_min, ts_max)
    }
}

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn pick<'a>(&mut self, items: &'a [&'a str]) -> &'a str {
        items[self.below(items.len() as u64) as usize]
    }
}

fn engine_with(trigrams: bool, reads: Arc<AtomicUsize>) -> BlockEngine {
    BlockEngine::new(
        Box::new(CountingStore {
            inner: MemBlockStore::new(),
            reads,
        }),
        BlockEngineConfig {
            flush_threshold: 1_000_000,
            message_trigrams: trigrams,
            ..Default::default()
        },
    )
    .unwrap()
}

const WORDS: &[&str] = &[
    "timeout",
    "TIMEOUT",
    "connection",
    "refused",
    "declined",
    "μs-latency",
    "ok",
    "xtimeouty",
    "time",
    "out",
    "%literal%",
    "under_score",
];

fn hostile_messages(rng: &mut Rng, n: usize, salt: &str) -> Vec<String> {
    (0..n)
        .map(|i| {
            format!(
                "{} {} {} {salt}{i}",
                rng.pick(WORDS),
                rng.pick(WORDS),
                rng.pick(WORDS)
            )
        })
        .collect()
}

/// The soundness property: pruned results == unpruned results, exactly,
/// across hostile messages (case variants, unicode, wildcard chars in
/// DATA, substrings straddling words) and hostile patterns.
#[test]
fn pruning_is_sound() {
    let reads = Arc::new(AtomicUsize::new(0));
    let engine = engine_with(true, reads.clone());
    let mut rng = Rng(0xF6F6_0001);

    // Several flushes → several blocks; then a buffered tail.
    let mut ts = 0i64;
    for round in 0..6 {
        for m in hostile_messages(&mut rng, 300, &format!("r{round}-")) {
            ts += 1 + rng.below(5) as i64;
            engine
                .push(LogEntry {
                    ts,
                    level: (rng.below(4)) as u8,
                    severity: None,
                    message: m,
                    metadata: vec![],
                    metadata_json: None,
                })
                .unwrap();
        }
        engine.flush().unwrap();
    }
    for m in hostile_messages(&mut rng, 100, "buffered-") {
        ts += 1;
        engine
            .push(LogEntry {
                ts,
                level: 1,
                severity: None,
                message: m,
                metadata: vec![],
                metadata_json: None,
            })
            .unwrap();
    }

    let patterns = [
        "%timeout%",
        "%TIMEOUT%",  // case folding
        "%time%out%", // multiple runs
        "%xtimeouty%",
        "%μs-latency%",         // multi-byte UTF-8
        "%refused connection%", // absent adjacency (words never adjacent in that order? maybe)
        "%r3-1%",               // salt hits one flush round
        "%zzz-not-there%",      // no matches at all
        "%ab%",                 // runs too short to prune
        "timeout%",             // anchored
        "%_eclined%",           // underscore wildcard
    ];
    for pattern in patterns {
        let base = LogQuery {
            ts_min: i64::MIN,
            ts_max: i64::MAX,
            level: None,
            severity: None,
            metadata_eq: vec![],
            message_contains: None,
            message_like_prune: None,
        };
        let unpruned = engine.query(&base).unwrap();
        let pruned = engine
            .query(&LogQuery {
                message_like_prune: Some(pattern.to_string()),
                ..base
            })
            .unwrap();
        // Soundness: every entry the pattern COULD match must survive
        // pruning. We compare the full candidate sets after applying an
        // independent LIKE evaluation to both.
        let matches = |entries: &[LogEntry]| -> Vec<(i64, String)> {
            entries
                .iter()
                .filter(|e| sql_like(pattern, &e.message))
                .map(|e| (e.ts, e.message.clone()))
                .collect()
        };
        assert_eq!(
            matches(&pruned),
            matches(&unpruned),
            "pattern {pattern:?}: pruning changed the LIKE result set"
        );
    }
}

/// Read-count proof: a selective pattern must decode FEWER blocks than
/// a full scan; a no-trigram engine must not prune at all.
#[test]
fn pruning_actually_prunes() {
    let reads = Arc::new(AtomicUsize::new(0));
    let engine = engine_with(true, reads.clone());
    // 5 blocks; the needle appears only in block 3's messages.
    let mut ts = 0i64;
    for round in 0..5 {
        for i in 0..200 {
            ts += 1;
            let message = if round == 3 {
                format!("needle-xyzzy present {i}")
            } else {
                format!("ordinary text {i}")
            };
            engine
                .push(LogEntry {
                    ts,
                    level: 1,
                    severity: None,
                    message,
                    metadata: vec![],
                    metadata_json: None,
                })
                .unwrap();
        }
        engine.flush().unwrap();
    }

    reads.store(0, Ordering::Relaxed);
    let q = LogQuery {
        ts_min: i64::MIN,
        ts_max: i64::MAX,
        level: None,
        severity: None,
        metadata_eq: vec![],
        message_contains: None,
        message_like_prune: Some("%xyzzy%".to_string()),
    };
    let hits = engine.query(&q).unwrap();
    let pruned_reads = reads.load(Ordering::Relaxed);
    assert_eq!(
        hits.iter().filter(|e| e.message.contains("xyzzy")).count(),
        200
    );
    assert_eq!(
        pruned_reads, 1,
        "only the needle block should decode (got {pruned_reads} reads)"
    );

    reads.store(0, Ordering::Relaxed);
    let unpruned = engine
        .query(&LogQuery {
            message_like_prune: None,
            ..q
        })
        .unwrap();
    assert_eq!(
        reads.load(Ordering::Relaxed),
        5,
        "full scan reads all blocks"
    );
    assert_eq!(unpruned.len(), 1000);
}

/// Blocks written WITHOUT the index (marker absent) are never pruned.
#[test]
fn unindexed_blocks_never_pruned() {
    let reads = Arc::new(AtomicUsize::new(0));
    // Same store contents, but engine had trigrams OFF at write time.
    let engine = engine_with(false, reads.clone());
    for i in 0..100 {
        engine
            .push(LogEntry {
                ts: i64::from(i) + 1,
                level: 1,
                severity: None,
                message: format!("secret-tokenword {i}"),
                metadata: vec![],
                metadata_json: None,
            })
            .unwrap();
    }
    engine.flush().unwrap();

    reads.store(0, Ordering::Relaxed);
    let hits = engine
        .query(&LogQuery {
            ts_min: i64::MIN,
            ts_max: i64::MAX,
            level: None,
            severity: None,
            metadata_eq: vec![],
            message_contains: None,
            message_like_prune: Some("%tokenword%".to_string()),
        })
        .unwrap();
    assert_eq!(hits.len(), 100, "unindexed block fully decoded");
    assert_eq!(reads.load(Ordering::Relaxed), 1);
}

/// Reference SQLite-LIKE evaluator (default semantics: ASCII case
/// folding, % and _ wildcards, no escape).
fn sql_like(pattern: &str, text: &str) -> bool {
    fn fold(b: u8) -> u8 {
        if b.is_ascii_uppercase() {
            b + 32
        } else {
            b
        }
    }
    fn rec(p: &[u8], t: &[u8]) -> bool {
        match p.first() {
            None => t.is_empty(),
            Some(b'%') => (0..=t.len()).any(|i| rec(&p[1..], &t[i..])),
            Some(b'_') => !t.is_empty() && rec(&p[1..], &t[1..]),
            Some(&c) => !t.is_empty() && fold(t[0]) == fold(c) && rec(&p[1..], &t[1..]),
        }
    }
    rec(pattern.as_bytes(), text.as_bytes())
}
