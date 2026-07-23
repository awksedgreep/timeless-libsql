//! SpanBlockEngine: buffer → raw span block → optimized block state
//! machine plus the query path — the traces twin of blocks/engine.rs,
//! kept deliberately diff-able against it (same method names, same
//! locking, same greedy merge). One instance per traces vtab.
//!
//! Concurrency model — identical, and identically NON-NEGOTIABLE: every
//! public method takes &self, guards state with Mutexes, and NOTHING in
//! here uses rayon or spawns threads (PLAN.md Session 3 lesson: store
//! calls re-enter SQLite on the host connection whose mutex the vtab
//! callback thread holds; a worker thread touching the store would
//! deadlock).
//!
//! Differences from BlockEngine, all traced to the trace-store design:
//!   - partition dimension is STATUS not level (3 pure buckets + mixed);
//!   - terms are always service:/kind:/status:/name: (no index_keys —
//!     see spans/mod.rs for why span dimensions need no allowlist);
//!   - every persisted block carries its deduped TRACE-ID set, and the
//!     query path has a second entrance: query() with a trace_id uses
//!     store.query_trace() instead of the term index.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::sync::Mutex;

use super::codec::{decode_span_block, encode_span_block, CODEC_COLUMNAR_V2, CODEC_RAW};
use super::{status_name, BlockLoc, BlockMeta, EncodedSpanBlock, SpanBlockStore, SpanEntry};

/// Tuning knobs. All ts_* values are in the SAME opaque unit as
/// SpanEntry.start_ts — the engine never assumes a unit (the traces
/// vtab feeds it nanoseconds and passes 1h-in-ns for the merge cap).
pub struct SpanEngineConfig {
    /// Buffered spans that trigger an automatic flush inside push().
    pub flush_threshold: usize,
    /// zstd level for compressed blocks (7 = the measured sweet spot;
    /// codec 4's per-column zstd strategies use it too).
    pub zstd_level: i32,
    /// optimize() aims for merged blocks of ~this many spans.
    pub merge_target_entries: usize,
    /// HARD CAP on the ts span of a MERGED block — the retention
    /// boundary rule (PLAN.md "Pruning & retention"), same as logs.
    /// Default uncapped (unit-agnostic engine can't pick a default).
    pub merge_max_ts_span: i64,
}

impl Default for SpanEngineConfig {
    fn default() -> Self {
        SpanEngineConfig {
            flush_threshold: 8192,
            zstd_level: 7,
            merge_target_entries: 8192,
            merge_max_ts_span: i64::MAX,
        }
    }
}

/// One query. ts range is always present (i64::MIN+1 / i64::MAX-1 for
/// "unbounded", like the other vtabs). `trace_id` switches the plan:
/// when set, candidate blocks come from the TRACE INDEX, not the term
/// posting lists — that is the hero pushdown.
pub struct SpanQuery {
    pub ts_min: i64,
    pub ts_max: i64,
    pub trace_id: Option<[u8; 16]>,
    pub service: Option<String>,
    /// Exact kind match (0..=4).
    pub kind: Option<u8>,
    /// Exact status match (0..=2).
    pub status: Option<u8>,
    /// Exact operation-name match.
    pub name: Option<String>,
}

/// One entry in the engine's in-memory block index: persisted metadata
/// plus the STATUS PARTITION tag — the same design as the logs
/// IndexEntry (read blocks/engine.rs for the full "level-term weakness"
/// story; status plays level's role here because 'find the failed
/// requests' is THE trace query and error-pure blocks make
/// `status:error` prune everything else).
///
/// The tag is IN-MEMORY ONLY, re-derived at recovery from the `status:`
/// posting lists (a block under exactly one status: term is pure, ≥2 is
/// mixed — three metadata-only query_terms calls, zero new persistence).
/// `Some(status)` = status-pure; `None` = mixed. Mixed blocks form
/// their own merge partition and never merge with pure ones.
#[derive(Clone, Copy, Debug)]
struct IndexEntry {
    meta: BlockMeta,
    loc: BlockLoc,
    partition: Option<u8>,
}

pub struct SpanBlockEngine {
    store: Box<dyn SpanBlockStore>,
    config: SpanEngineConfig,
    /// Spans pushed but not yet flushed. Queryable (same
    /// queryable-before-flush property as every timeless engine).
    buffer: Mutex<Vec<SpanEntry>>,
    /// In-memory metadata index of every persisted block; optimize()
    /// and prune() plan from this, the query path asks the store.
    index: Mutex<Vec<IndexEntry>>,
}

impl SpanBlockEngine {
    /// Construct over a store, recovering the block index via scan()
    /// and each block's status partition via the `status:` posting
    /// lists (see IndexEntry). Safe to call re-entrantly from
    /// xCreate/xConnect — the calling thread already holds the host
    /// connection.
    pub fn new(store: Box<dyn SpanBlockStore>, config: SpanEngineConfig) -> Result<Self, String> {
        let scanned = store.scan()?;

        // Partition derivation: which blocks carry each of the three
        // status: terms? Exactly one hit = status-pure, several = mixed
        // (0 hits should be impossible — every block emits at least one
        // status term — but is treated as mixed, the conservative
        // bucket, rather than guessed at).
        let mut hits: HashMap<i64, (u32, u8)> = HashMap::new(); // id → (count, last status)
        for st in 0u8..3 {
            let term = vec![format!("status:{}", status_name(st))];
            for (loc, _) in store.query_terms(&term, i64::MIN, i64::MAX)? {
                let e = hits.entry(loc.id).or_insert((0, st));
                e.0 += 1;
                e.1 = st;
            }
        }
        let index = scanned
            .into_iter()
            .map(|(meta, loc)| IndexEntry {
                meta,
                loc,
                partition: match hits.get(&loc.id) {
                    Some((1, st)) => Some(*st),
                    _ => None,
                },
            })
            .collect();

        Ok(SpanBlockEngine {
            store,
            config,
            buffer: Mutex::new(Vec::new()),
            index: Mutex::new(index),
        })
    }

    pub fn config(&self) -> &SpanEngineConfig {
        &self.config
    }

    /// Poison-tolerant locks, same style as the rest of timeless-core.
    fn buffer_lock(&self) -> std::sync::MutexGuard<'_, Vec<SpanEntry>> {
        self.buffer.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn index_lock(&self) -> std::sync::MutexGuard<'_, Vec<IndexEntry>> {
        self.index.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Append one span. Validates kind/status, sorts attributes
    /// (canonical order; last duplicate key wins, matching the logs
    /// metadata convention), auto-flushes at the threshold.
    pub fn push(&self, mut entry: SpanEntry) -> Result<(), String> {
        if entry.kind > 4 {
            return Err(format!(
                "invalid span kind {} (0=internal 1=server 2=client 3=producer 4=consumer)",
                entry.kind
            ));
        }
        if entry.status > 2 {
            return Err(format!(
                "invalid span status {} (0=unset 1=ok 2=error)",
                entry.status
            ));
        }
        entry.attributes.sort_by(|a, b| a.0.cmp(&b.0));
        entry.attributes.reverse(); // last duplicates first...
        entry.attributes.dedup_by(|a, b| a.0 == b.0); // ...survive dedup
        entry.attributes.reverse(); // back to ascending key order

        let should_flush = {
            let mut buf = self.buffer_lock();
            buf.push(entry);
            buf.len() >= self.config.flush_threshold
        };
        if should_flush {
            self.flush()?;
        }
        Ok(())
    }

    pub fn buffered_count(&self) -> usize {
        self.buffer_lock().len()
    }

    /// Drain the buffer into RAW blocks — STATUS-PARTITIONED, exactly
    /// like the logs level-partitioned flush: sort by (status,
    /// start_ts), write one status-PURE block per status present (≤3),
    /// so each block emits exactly one `status:` term and the posting-
    /// list intersection prunes perfectly. Each block also records its
    /// deduped trace-id set for the trace index — created in the same
    /// put_blocks operation as the block rows (never dangles). Returns
    /// the number of spans flushed.
    pub fn flush(&self) -> Result<usize, String> {
        // Hold the buffer lock for the whole flush (single-threaded in
        // the vtab anyway; correctness is free). The buffer stays
        // intact if any encode or store call fails.
        let mut buf = self.buffer_lock();
        if buf.is_empty() {
            return Ok(0);
        }
        buf.sort_by_key(|e| (e.status, e.start_ts));

        let mut blocks: Vec<EncodedSpanBlock> = Vec::new();
        let mut statuses: Vec<u8> = Vec::new(); // partition tag per block
        let mut start = 0usize;
        while start < buf.len() {
            let status = buf[start].status;
            let end = start + buf[start..].iter().take_while(|e| e.status == status).count();
            let run = &buf[start..end];
            let (data, meta) = encode_span_block(run, CODEC_RAW, self.config.zstd_level)?;
            blocks.push(EncodedSpanBlock {
                meta,
                data,
                terms: extract_terms(run),
                trace_ids: extract_trace_ids(run),
            });
            statuses.push(status);
            start = end;
        }

        let locs = self.store.put_blocks(&blocks)?;
        {
            let mut index = self.index_lock();
            for ((block, loc), status) in blocks.iter().zip(&locs).zip(&statuses) {
                index.push(IndexEntry {
                    meta: block.meta,
                    loc: *loc,
                    partition: Some(*status),
                });
            }
        }
        let n = buf.len();
        buf.clear();
        Ok(n)
    }

    /// Two-tier compaction ('optimize' command) — the same pass as
    /// blocks/engine.rs::optimize, with STATUS partitions: raw blocks
    /// are recompressed to CODEC_COLUMNAR_V2 (codec 5, adaptive
    /// per-column strategies + shredded attributes — legacy codec-2/4
    /// blocks stay decodable and upgrade whenever a merge rewrites
    /// them), small blocks merge toward
    /// merge_target_entries WITHIN their status partition only (merging
    /// an error-pure block into an ok-pure one would re-create exactly
    /// the mixed blocks the partitioned flush prevents), subject to the
    /// merge_max_ts_span retention-boundary cap, all in ONE
    /// replace_blocks call (the SQLite backend rides one host
    /// transaction: new blocks + terms + trace rows in, old ones out,
    /// atomically). Merged blocks recompute BOTH index row sets from
    /// the merged spans. Returns (blocks_removed, blocks_written).
    pub fn optimize(&self) -> Result<(usize, usize), String> {
        let candidates: Vec<IndexEntry> = self
            .index_lock()
            .iter()
            .filter(|e| {
                e.meta.codec == CODEC_RAW
                    || (e.meta.entry_count as usize) < self.config.merge_target_entries
            })
            .copied()
            .collect();
        if candidates.is_empty() {
            return Ok((0, 0));
        }

        // One bucket per pure status (0..=2) plus one for mixed legacy
        // blocks; the greedy time-locality grouping runs INSIDE each.
        let mut buckets: [Vec<IndexEntry>; 4] = Default::default();
        for e in candidates {
            let b = match e.partition {
                Some(st) => st as usize,
                None => 3, // the mixed bucket
            };
            buckets[b].push(e);
        }

        let mut groups: Vec<(Vec<IndexEntry>, Option<u8>)> = Vec::new();
        for (b, bucket) in buckets.iter_mut().enumerate() {
            if bucket.is_empty() {
                continue;
            }
            let partition = if b < 3 { Some(b as u8) } else { None };
            bucket.sort_by_key(|e| (e.meta.ts_min, e.meta.ts_max));

            // Greedy grouping under two constraints: target span count
            // and the merged-span hard cap (saturating_sub: spans near
            // i64 extremes must not wrap).
            let mut cur: Vec<IndexEntry> = Vec::new();
            let mut cur_entries = 0usize;
            let (mut cur_min, mut cur_max) = (0i64, 0i64);
            for e in bucket.drain(..) {
                let m = e.meta;
                let fits = if cur.is_empty() {
                    true
                } else {
                    let new_min = cur_min.min(m.ts_min);
                    let new_max = cur_max.max(m.ts_max);
                    let span_ok =
                        new_max.saturating_sub(new_min) <= self.config.merge_max_ts_span;
                    let size_ok = cur_entries + m.entry_count as usize
                        <= self.config.merge_target_entries;
                    span_ok && size_ok
                };
                if !fits {
                    groups.push((std::mem::take(&mut cur), partition));
                    cur_entries = 0;
                }
                if cur.is_empty() {
                    cur_min = m.ts_min;
                    cur_max = m.ts_max;
                } else {
                    cur_min = cur_min.min(m.ts_min);
                    cur_max = cur_max.max(m.ts_max);
                }
                cur_entries += m.entry_count as usize;
                cur.push(e);
            }
            if !cur.is_empty() {
                groups.push((std::mem::take(&mut cur), partition));
            }
        }

        // Rewrite-worthiness rule unchanged from logs: any RAW block in
        // the group (must transition to zstd) or ≥2 blocks (an actual
        // merge); a lone small zstd block is left alone (rewriting it
        // is write amplification for zero gain). Sequential reads on
        // THIS thread — module header.
        let mut adds: Vec<EncodedSpanBlock> = Vec::new();
        let mut add_partitions: Vec<Option<u8>> = Vec::new();
        let mut removes: Vec<BlockLoc> = Vec::new();
        for (group, partition) in &groups {
            let worth_it =
                group.len() >= 2 || group.iter().any(|e| e.meta.codec == CODEC_RAW);
            if !worth_it {
                continue;
            }
            let mut entries: Vec<SpanEntry> = Vec::new();
            for e in group {
                let bytes = self.store.read_block(&e.loc)?;
                entries.extend(decode_span_block(&bytes)?);
            }
            entries.sort_by_key(|e| e.start_ts);
            let terms = extract_terms(&entries);
            let trace_ids = extract_trace_ids(&entries);
            let (data, meta) =
                encode_span_block(&entries, CODEC_COLUMNAR_V2, self.config.zstd_level)?;
            adds.push(EncodedSpanBlock {
                meta,
                data,
                terms,
                trace_ids,
            });
            add_partitions.push(*partition);
            removes.extend(group.iter().map(|e| e.loc));
        }
        if adds.is_empty() {
            return Ok((0, 0));
        }

        // One atomic swap; the callback rewrites the in-memory index at
        // the moment both generations exist in the store.
        let add_metas: Vec<BlockMeta> = adds.iter().map(|b| b.meta).collect();
        let removed = removes.len();
        self.store
            .replace_blocks(&adds, &removes, &mut |new_locs: &[BlockLoc]| {
                let mut index = self.index_lock();
                index.retain(|e| !removes.contains(&e.loc));
                for ((meta, loc), partition) in
                    add_metas.iter().zip(new_locs).zip(&add_partitions)
                {
                    index.push(IndexEntry {
                        meta: *meta,
                        loc: *loc,
                        partition: *partition,
                    });
                }
            })?;
        Ok((removed, add_metas.len()))
    }

    /// Retention: delete every block whose ts_max < cutoff plus any
    /// buffered spans older than the cutoff. The store removes term AND
    /// trace-index rows in the same operation (never-dangle rule).
    /// Returns the number of blocks deleted.
    pub fn prune(&self, cutoff: i64) -> Result<usize, String> {
        let victims: Vec<BlockLoc> = self
            .index_lock()
            .iter()
            .filter(|e| e.meta.ts_max < cutoff)
            .map(|e| e.loc)
            .collect();
        self.buffer_lock().retain(|e| e.start_ts >= cutoff);
        if victims.is_empty() {
            return Ok(0);
        }
        let errors = self.store.delete_blocks(&victims);
        if !errors.is_empty() {
            return Err(format!("prune errors: {}", errors.join("; ")));
        }
        self.index_lock().retain(|e| e.meta.ts_max >= cutoff);
        Ok(victims.len())
    }

    /// The query path — NO rayon, sequential reads (module header).
    ///
    /// TWO plans, chosen by the presence of trace_id:
    ///   trace plan: store.query_trace() → exactly the blocks holding
    ///     that trace's spans (the hero pushdown; the ts overlap check
    ///     on the returned metas still applies — free pruning);
    ///   term plan: service/kind/status/name filters → terms →
    ///     store.query_terms (posting-list intersection + ts overlap).
    /// Then, both plans: read + decode candidates, exact per-span
    /// filtering (block-granular indexes approximate), merge matching
    /// BUFFERED spans (queryable-before-flush), sort by start_ts.
    pub fn query(&self, q: &SpanQuery) -> Result<Vec<SpanEntry>, String> {
        if let Some(k) = q.kind {
            if k > 4 {
                return Err(format!("invalid kind {k} in query"));
            }
        }
        if let Some(s) = q.status {
            if s > 2 {
                return Err(format!("invalid status {s} in query"));
            }
        }

        let locs = match &q.trace_id {
            Some(tid) => self
                .store
                .query_trace(tid)?
                .into_iter()
                .filter(|(_, m)| m.ts_min <= q.ts_max && m.ts_max >= q.ts_min)
                .collect::<Vec<_>>(),
            None => {
                let mut terms: Vec<String> = Vec::new();
                if let Some(svc) = &q.service {
                    terms.push(format!("service:{svc}"));
                }
                if let Some(k) = q.kind {
                    terms.push(format!("kind:{}", super::kind_name(k)));
                }
                if let Some(s) = q.status {
                    terms.push(format!("status:{}", status_name(s)));
                }
                if let Some(n) = &q.name {
                    terms.push(format!("name:{n}"));
                }
                self.store.query_terms(&terms, q.ts_min, q.ts_max)?
            }
        };

        let mut out: Vec<SpanEntry> = Vec::new();
        for (loc, _meta) in &locs {
            let bytes = self.store.read_block(loc)?;
            for entry in decode_span_block(&bytes)? {
                if entry_matches(&entry, q) {
                    out.push(entry);
                }
            }
        }
        for entry in self.buffer_lock().iter() {
            if entry_matches(entry, q) {
                out.push(entry.clone());
            }
        }
        out.sort_by_key(|e| e.start_ts);
        Ok(out)
    }

    /// (persisted blocks, raw blocks, buffered spans) — cheap, index-only.
    pub fn stats(&self) -> (usize, usize, usize) {
        let index = self.index_lock();
        let raw = index.iter().filter(|e| e.meta.codec == CODEC_RAW).count();
        (index.len(), raw, self.buffered_count())
    }
}

/// Terms for a batch of spans: service:/kind:/status:/name: — ALWAYS
/// all four (no index_keys allowlist; spans/mod.rs explains why traces
/// differ from logs here). Deduplicated + sorted; a block-level index
/// only cares that the term occurs at all.
fn extract_terms(entries: &[SpanEntry]) -> Vec<String> {
    let mut set = BTreeSet::new();
    for e in entries {
        set.insert(format!("service:{}", e.service));
        set.insert(format!("kind:{}", super::kind_name(e.kind)));
        set.insert(format!("status:{}", status_name(e.status)));
        set.insert(format!("name:{}", e.name));
    }
    set.into_iter().collect()
}

/// Deduped, sorted trace ids of a batch — the block's trace-index rows.
/// BTreeSet gives dedup + deterministic order in one move ([u8;16] is
/// Ord); a trace with many spans in one block still costs one row.
fn extract_trace_ids(entries: &[SpanEntry]) -> Vec<[u8; 16]> {
    let mut set = BTreeSet::new();
    for e in entries {
        set.insert(e.trace_id);
    }
    set.into_iter().collect()
}

/// Exact per-span filter — the truth the block-level indexes only
/// approximate (a block containing the trace still contains other
/// traces' spans; a status-pure block still spans a ts range).
fn entry_matches(e: &SpanEntry, q: &SpanQuery) -> bool {
    if e.start_ts < q.ts_min || e.start_ts > q.ts_max {
        return false;
    }
    if let Some(tid) = &q.trace_id {
        if &e.trace_id != tid {
            return false;
        }
    }
    if let Some(svc) = &q.service {
        if &e.service != svc {
            return false;
        }
    }
    if let Some(k) = q.kind {
        if e.kind != k {
            return false;
        }
    }
    if let Some(s) = q.status {
        if e.status != s {
            return false;
        }
    }
    if let Some(n) = &q.name {
        if &e.name != n {
            return false;
        }
    }
    true
}
