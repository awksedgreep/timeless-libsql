//! An in-memory `ChunkStore` that answers every question the engine asks of
//! a store: chunk rows, the series catalog, the label index, and the raw and
//! rollup chunk indexes.
//!
//! It is the engine's test double for the SQLite shadow-table store, and
//! behaves like it where the engine can tell the difference:
//!
//! - a chunk is a row with an id, and a freed id is given to the next row
//!   (SQLite's rule for an `INTEGER PRIMARY KEY`: one more than the largest
//!   in use), so a plan that outlives its rows meets another chunk under
//!   the same id, as it would there;
//! - writes belong to a transaction when one is open: [`MemStore::begin`],
//!   [`MemStore::commit`] and [`MemStore::rollback`] stand in for the host
//!   database's, and nest as savepoints do;
//! - clones share one store, as two connections share one database, so a
//!   test can drop an engine and open another over what it wrote.
//!
//! Nothing here is indexed for speed: every query walks the rows. It holds
//! what a test writes.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

use super::{
    ChunkBytes, ChunkIdentity, ChunkLoc, ChunkMeta, ChunkStore, EncodedChunk, EncodedRollupChunk,
    ResolvedSeries, StoredChunk, StoredRollupChunk, StoredSeries,
};
use crate::rollup::ENC_ROLLUP_V1;

/// The label key a series' metric name is indexed under.
const NAME_KEY: &str = "__name__";

/// The `index_cache` budget of a store made with [`MemStore::new`]: the
/// extension's default for a table it creates.
const DEFAULT_BUDGET: u64 = 64 << 20;

#[derive(Clone)]
struct Row {
    series_id: i64,
    /// 0 for a raw chunk, a tier's bucket width for a rollup chunk.
    resolution: i64,
    min_ts: i64,
    max_ts: i64,
    max_ts_val: Option<f64>,
    point_count: u32,
    min_val: f64,
    max_val: f64,
    sum_val: f64,
    encoding: u8,
    ts: Vec<u8>,
    val: Vec<u8>,
}

impl Row {
    fn meta(&self, rowid: i64) -> ChunkMeta {
        ChunkMeta {
            min_ts: self.min_ts,
            max_ts: self.max_ts,
            max_ts_val: self.max_ts_val,
            point_count: self.point_count,
            payload_bytes: (self.ts.len() + self.val.len()) as u64,
            min_val: self.min_val,
            max_val: self.max_val,
            sum_val: self.sum_val,
            loc: ChunkLoc::Row { rowid },
            encoding: self.encoding,
        }
    }
}

type Labels = Vec<(String, String)>;

#[derive(Clone, Default)]
struct State {
    /// Chunk rows by id.
    rows: BTreeMap<i64, Row>,
    /// The catalog: a series' name and its labels, sorted by key.
    series: BTreeMap<i64, (String, Labels)>,
    by_identity: HashMap<(String, Labels), i64>,
    /// Moves on every change to the rows or the catalog.
    generation: i64,
    /// Moves on every change that removes or replaces a row.
    shape_generation: i64,
    registry: Option<Vec<u8>>,
}

impl State {
    fn insert_row(&mut self, row: Row) -> i64 {
        let rowid = self.rows.keys().next_back().map_or(1, |last| last + 1);
        self.rows.insert(rowid, row);
        self.generation += 1;
        rowid
    }

    fn raw(&self) -> impl Iterator<Item = (i64, &Row)> {
        self.rows
            .iter()
            .filter(|(_, row)| row.resolution == 0)
            .map(|(rowid, row)| (*rowid, row))
    }

    fn rollups(&self) -> impl Iterator<Item = (i64, &Row)> {
        self.rows
            .iter()
            .filter(|(_, row)| row.resolution > 0)
            .map(|(rowid, row)| (*rowid, row))
    }

    /// Whether a series is `metric` (when given) and carries every pair of
    /// `eq` and, for each entry of `any_of`, one of its values.
    fn matches(
        name: &str,
        labels: &Labels,
        metric: Option<&str>,
        eq: &[(String, String)],
        any_of: &[(String, Vec<String>)],
    ) -> bool {
        let value_of = |key: &str| -> Option<&str> {
            if key == NAME_KEY {
                return Some(name);
            }
            labels
                .iter()
                .find(|(k, _)| k == key)
                .map(|(_, value)| value.as_str())
        };
        metric.is_none_or(|metric| metric == name)
            && eq
                .iter()
                .all(|(key, value)| value_of(key) == Some(value.as_str()))
            && any_of.iter().all(|(key, values)| {
                value_of(key).is_some_and(|value| values.iter().any(|v| v == value))
            })
    }
}

fn row_id(loc: &ChunkLoc) -> Result<i64, String> {
    match loc {
        ChunkLoc::Row { rowid } => Ok(*rowid),
        ChunkLoc::File { .. } => Err("in-memory store chunks are rows".to_string()),
    }
}

fn sorted(labels: &[(String, String)]) -> Labels {
    let mut labels = labels.to_vec();
    labels.sort();
    labels
}

/// A failure a test can have the store make, to see what the engine does
/// with it. Stays in force until another is injected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemFault {
    None,
    /// A batch read (`read_chunks`) fails.
    BatchReadFails,
    /// A batch read returns one payload fewer than it was asked for.
    BatchReadShort,
    /// A compaction swap (`replace_chunks`) fails before writing anything.
    ReplaceFails,
}

/// See the module documentation.
#[derive(Clone)]
pub struct MemStore {
    state: Arc<Mutex<State>>,
    /// The state as it was at each open `begin`, outermost first.
    undo: Arc<Mutex<Vec<State>>>,
    /// `None` reads as a table with no `index_cache` setting.
    budget: Option<u64>,
    fault: Arc<Mutex<MemFault>>,
}

impl Default for MemStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemStore {
    /// An empty store whose index is read from the store, as a table made
    /// by the extension with its default `index_cache` is.
    pub fn new() -> Self {
        Self::with_budget(Some(DEFAULT_BUDGET))
    }

    /// The store kept under `name`, made empty the first time it is asked
    /// for: what a path is to a database file. A test that drops an engine
    /// and opens another under the same name reads what the first wrote.
    pub fn named(name: &std::path::Path) -> Self {
        static STORES: std::sync::LazyLock<Mutex<HashMap<std::path::PathBuf, MemStore>>> =
            std::sync::LazyLock::new(|| Mutex::new(HashMap::new()));
        STORES
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .entry(name.to_path_buf())
            .or_default()
            .clone()
    }

    /// An empty store with the given `index_cache` budget.
    pub fn with_budget(budget: Option<u64>) -> Self {
        MemStore {
            state: Arc::new(Mutex::new(State::default())),
            undo: Arc::new(Mutex::new(Vec::new())),
            budget,
            fault: Arc::new(Mutex::new(MemFault::None)),
        }
    }

    /// Have this store, and every clone of it, fail in the given way from
    /// now on.
    pub fn inject(&self, fault: MemFault) {
        *self.fault.lock().unwrap_or_else(|error| error.into_inner()) = fault;
    }

    fn fault(&self) -> MemFault {
        *self.fault.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn undo_lock(&self) -> MutexGuard<'_, Vec<State>> {
        self.undo.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// Open a transaction, or a savepoint inside one.
    pub fn begin(&self) {
        let snapshot = self.lock().clone();
        self.undo_lock().push(snapshot);
    }

    /// Keep what the innermost open transaction wrote.
    pub fn commit(&self) {
        self.undo_lock().pop();
    }

    /// Undo what the innermost open transaction wrote.
    pub fn rollback(&self) {
        if let Some(snapshot) = self.undo_lock().pop() {
            *self.lock() = snapshot;
        }
    }

    /// How many chunk rows there are, raw and rollup.
    pub fn row_count(&self) -> usize {
        self.lock().rows.len()
    }

    /// The ids of every chunk row, ascending.
    pub fn row_ids(&self) -> Vec<i64> {
        self.lock().rows.keys().copied().collect()
    }

    fn rollup_chunk(rowid: i64, row: &Row) -> StoredRollupChunk {
        StoredRollupChunk {
            series_id: row.series_id,
            resolution: row.resolution,
            meta: ChunkMeta {
                payload_bytes: 0,
                ..row.meta(rowid)
            },
        }
    }
}

impl ChunkStore for MemStore {
    fn put_chunks(&self, chunks: &[EncodedChunk]) -> Result<Vec<ChunkLoc>, String> {
        let mut state = self.lock();
        Ok(chunks
            .iter()
            .map(|chunk| ChunkLoc::Row {
                rowid: state.insert_row(Row {
                    series_id: chunk.series_id,
                    resolution: 0,
                    min_ts: chunk.min_ts,
                    max_ts: chunk.max_ts,
                    max_ts_val: Some(chunk.max_ts_val),
                    point_count: chunk.point_count,
                    min_val: chunk.min_val,
                    max_val: chunk.max_val,
                    sum_val: chunk.sum_val,
                    encoding: chunk.encoding,
                    ts: chunk.ts_bytes.clone(),
                    val: chunk.val_bytes.clone(),
                }),
            })
            .collect())
    }

    fn chunks_are_rows(&self) -> bool {
        true
    }

    fn replace_chunks(
        &self,
        add: &[EncodedChunk],
        remove: &[ChunkLoc],
        on_committed: &mut dyn FnMut(&[ChunkLoc]),
    ) -> Result<Vec<ChunkLoc>, String> {
        if self.fault() == MemFault::ReplaceFails {
            return Err("injected compaction replacement failure".to_string());
        }
        let locs = self.put_chunks(add)?;
        on_committed(&locs);
        let errors = self.delete_chunks(remove);
        if errors.is_empty() {
            Ok(locs)
        } else {
            Err(errors.join("; "))
        }
    }

    fn read_chunk(&self, loc: &ChunkLoc) -> Result<ChunkBytes, String> {
        let rowid = row_id(loc)?;
        let state = self.lock();
        let row = state
            .rows
            .get(&rowid)
            .ok_or_else(|| format!("chunk row {rowid}: Query returned no rows"))?;
        let mut data = Vec::with_capacity(row.ts.len() + row.val.len());
        data.extend_from_slice(&row.ts);
        data.extend_from_slice(&row.val);
        Ok(ChunkBytes {
            data: Arc::new(data),
            ts_range: 0..row.ts.len(),
            val_range: row.ts.len()..row.ts.len() + row.val.len(),
        })
    }

    fn read_chunks(&self, locs: &[ChunkLoc]) -> Result<Vec<ChunkBytes>, String> {
        let fault = self.fault();
        if fault == MemFault::BatchReadFails {
            return Err("injected batch read failure".to_string());
        }
        let mut payloads: Vec<ChunkBytes> = locs
            .iter()
            .map(|loc| self.read_chunk(loc))
            .collect::<Result<_, _>>()?;
        if fault == MemFault::BatchReadShort {
            payloads.pop();
        }
        Ok(payloads)
    }

    fn delete_chunks(&self, locs: &[ChunkLoc]) -> Vec<String> {
        let mut state = self.lock();
        let mut errors = Vec::new();
        for loc in locs {
            match row_id(loc) {
                Ok(rowid) => {
                    state.rows.remove(&rowid);
                }
                Err(error) => errors.push(error),
            }
        }
        if !locs.is_empty() {
            state.generation += 1;
            state.shape_generation += 1;
        }
        errors
    }

    fn scan(&self) -> Result<Vec<StoredChunk>, String> {
        Ok(self
            .lock()
            .raw()
            .map(|(rowid, row)| StoredChunk {
                series_id: row.series_id,
                meta: row.meta(rowid),
            })
            .collect())
    }

    fn save_registry(&self, bytes: &[u8]) -> Result<(), String> {
        self.lock().registry = Some(bytes.to_vec());
        Ok(())
    }

    fn load_registry(&self) -> Result<Option<Vec<u8>>, String> {
        Ok(self.lock().registry.clone())
    }

    // ── Series catalog ───────────────────────────────────────────────

    fn has_authoritative_series(&self) -> bool {
        true
    }

    fn load_series(&self) -> Result<Vec<StoredSeries>, String> {
        Ok(self
            .lock()
            .series
            .iter()
            .map(|(id, (name, labels))| StoredSeries {
                id: *id,
                name: name.clone(),
                labels: labels.clone(),
            })
            .collect())
    }

    fn load_series_since(&self, after_id: i64) -> Result<Vec<StoredSeries>, String> {
        Ok(self
            .load_series()?
            .into_iter()
            .filter(|series| series.id > after_id)
            .collect())
    }

    fn resolve_series(
        &self,
        name: &str,
        labels: &[(String, String)],
    ) -> Result<ResolvedSeries, String> {
        let mut state = self.lock();
        let identity = (name.to_owned(), sorted(labels));
        if let Some(id) = state.by_identity.get(&identity) {
            return Ok(ResolvedSeries {
                id: *id,
                created: false,
            });
        }
        let id = state.series.keys().next_back().map_or(1, |last| last + 1);
        state.series.insert(id, identity.clone());
        state.by_identity.insert(identity, id);
        state.generation += 1;
        Ok(ResolvedSeries { id, created: true })
    }

    fn delete_series(&self, ids: &[i64]) -> Result<(), String> {
        let mut state = self.lock();
        for id in ids {
            if let Some(identity) = state.series.remove(id) {
                state.by_identity.remove(&identity);
            }
        }
        if !ids.is_empty() {
            state.generation += 1;
        }
        Ok(())
    }

    fn migrate_series(&self, series: &[StoredSeries]) -> Result<(), String> {
        let mut state = self.lock();
        for row in series {
            let identity = (row.name.clone(), sorted(&row.labels));
            match state.by_identity.get(&identity) {
                Some(id) if *id == row.id => {}
                Some(id) => {
                    return Err(format!(
                        "series {:?} is already {id}, not {}",
                        row.name, row.id
                    ))
                }
                None => {
                    if state.series.contains_key(&row.id) {
                        return Err(format!("series id {} is taken", row.id));
                    }
                    state.series.insert(row.id, identity.clone());
                    state.by_identity.insert(identity, row.id);
                    state.generation += 1;
                }
            }
        }
        Ok(())
    }

    // ── Label index ──────────────────────────────────────────────────

    fn has_label_index(&self) -> bool {
        true
    }

    fn series_index_budget(&self) -> Result<Option<u64>, String> {
        Ok(self.budget)
    }

    fn series_ids(&self) -> Result<Vec<i64>, String> {
        Ok(self.lock().series.keys().copied().collect())
    }

    fn series_total(&self) -> Result<usize, String> {
        Ok(self.lock().series.len())
    }

    fn ensure_label_index(&self) -> Result<(), String> {
        Ok(())
    }

    fn label_index_current(&self) -> Result<bool, String> {
        Ok(true)
    }

    fn find_series_ids(
        &self,
        metric: Option<&str>,
        eq: &[(String, String)],
    ) -> Result<Vec<i64>, String> {
        self.find_series_ids_any(metric, eq, &[])
    }

    fn find_series_ids_any(
        &self,
        metric: Option<&str>,
        eq: &[(String, String)],
        any_of: &[(String, Vec<String>)],
    ) -> Result<Vec<i64>, String> {
        Ok(self
            .lock()
            .series
            .iter()
            .filter(|(_, (name, labels))| State::matches(name, labels, metric, eq, any_of))
            .map(|(id, _)| *id)
            .collect())
    }

    fn series_by_ids(&self, ids: &[i64]) -> Result<Vec<StoredSeries>, String> {
        let state = self.lock();
        Ok(ids
            .iter()
            .filter_map(|id| {
                state.series.get(id).map(|(name, labels)| StoredSeries {
                    id: *id,
                    name: name.clone(),
                    labels: labels.clone(),
                })
            })
            .collect())
    }

    fn metric_names(&self) -> Result<Vec<String>, String> {
        let names: BTreeSet<String> = self
            .lock()
            .series
            .values()
            .map(|(name, _)| name.clone())
            .collect();
        Ok(names.into_iter().collect())
    }

    fn label_values(&self, metric: Option<&str>, key: &str) -> Result<Vec<String>, String> {
        let state = self.lock();
        let values: BTreeSet<String> = state
            .series
            .values()
            .filter(|(name, _)| metric.is_none_or(|metric| metric == name))
            .filter_map(|(name, labels)| {
                if key == NAME_KEY {
                    return Some(name.clone());
                }
                labels
                    .iter()
                    .find(|(k, _)| k == key)
                    .map(|(_, value)| value.clone())
            })
            .collect();
        Ok(values.into_iter().collect())
    }

    fn label_names(&self, metric: Option<&str>) -> Result<Vec<String>, String> {
        let state = self.lock();
        let mut of_metric = state
            .series
            .values()
            .filter(|(name, _)| metric.is_none_or(|metric| metric == name))
            .peekable();
        // A metric nothing is stored for has no label names; with no
        // metric given, `__name__` is one even of an empty catalog.
        if metric.is_some() && of_metric.peek().is_none() {
            return Ok(Vec::new());
        }
        let mut names: BTreeSet<String> = BTreeSet::from([NAME_KEY.to_owned()]);
        for (_, labels) in of_metric {
            names.extend(labels.iter().map(|(key, _)| key.clone()));
        }
        Ok(names.into_iter().collect())
    }

    // ── Raw chunk index ──────────────────────────────────────────────

    fn has_chunk_index(&self) -> bool {
        true
    }

    fn chunk_metas_for(
        &self,
        series_ids: &[i64],
        t_start: i64,
        t_end: i64,
    ) -> Result<Vec<StoredChunk>, String> {
        let wanted: BTreeSet<i64> = series_ids.iter().copied().collect();
        let state = self.lock();
        let mut chunks: Vec<(i64, i64, i64, ChunkMeta)> = state
            .raw()
            .filter(|(_, row)| {
                wanted.contains(&row.series_id) && row.min_ts <= t_end && row.max_ts >= t_start
            })
            .map(|(rowid, row)| (row.series_id, row.min_ts, rowid, row.meta(rowid)))
            .collect();
        chunks.sort_by_key(|(series_id, min_ts, rowid, _)| (*series_id, *min_ts, *rowid));
        Ok(chunks
            .into_iter()
            .map(|(series_id, _, _, meta)| StoredChunk { series_id, meta })
            .collect())
    }

    fn chunk_stats_for(
        &self,
        series_ids: Option<&[i64]>,
    ) -> Result<Vec<(i64, i64, i64, u64, usize)>, String> {
        let wanted: Option<BTreeSet<i64>> = series_ids.map(|ids| ids.iter().copied().collect());
        let state = self.lock();
        let mut stats: BTreeMap<i64, (i64, i64, u64, usize)> = BTreeMap::new();
        for (_, row) in state.raw() {
            if wanted
                .as_ref()
                .is_some_and(|wanted| !wanted.contains(&row.series_id))
            {
                continue;
            }
            let entry = stats
                .entry(row.series_id)
                .or_insert((row.min_ts, row.max_ts, 0, 0));
            entry.0 = entry.0.min(row.min_ts);
            entry.1 = entry.1.max(row.max_ts);
            entry.2 += u64::from(row.point_count);
            entry.3 += 1;
        }
        Ok(stats
            .into_iter()
            .map(|(id, (min_ts, max_ts, points, chunks))| (id, min_ts, max_ts, points, chunks))
            .collect())
    }

    fn chunk_totals(&self) -> Result<(usize, u64, Option<i64>, Option<i64>), String> {
        let state = self.lock();
        let mut totals = (0usize, 0u64, None::<i64>, None::<i64>);
        for (_, row) in state.raw() {
            totals.0 += 1;
            totals.1 += u64::from(row.point_count);
            totals.2 = Some(totals.2.map_or(row.min_ts, |ts| ts.min(row.min_ts)));
            totals.3 = Some(totals.3.map_or(row.max_ts, |ts| ts.max(row.max_ts)));
        }
        Ok(totals)
    }

    fn chunk_series_ids(&self) -> Result<Vec<i64>, String> {
        let ids: BTreeSet<i64> = self.lock().raw().map(|(_, row)| row.series_id).collect();
        Ok(ids.into_iter().collect())
    }

    fn chunk_locs_outside(
        &self,
        before: Option<i64>,
        after: Option<i64>,
    ) -> Result<Vec<ChunkLoc>, String> {
        let state = self.lock();
        let outside = |row: &Row| match (before, after) {
            (Some(before), None) => Ok(row.max_ts < before),
            (None, Some(after)) => Ok(row.min_ts > after),
            _ => Err("chunk_locs_outside takes exactly one bound".to_string()),
        };
        let mut locs = Vec::new();
        for (rowid, row) in state.raw() {
            if outside(row)? {
                locs.push(ChunkLoc::Row { rowid });
            }
        }
        Ok(locs)
    }

    fn series_with_raw_between(
        &self,
        series: &[(i64, Option<i64>)],
        upto: i64,
    ) -> Result<Vec<i64>, String> {
        let state = self.lock();
        Ok(series
            .iter()
            .filter(|(series_id, covered_to)| {
                state.raw().any(|(_, row)| {
                    row.series_id == *series_id
                        && row.min_ts <= upto
                        && covered_to.is_none_or(|covered_to| row.max_ts > covered_to)
                })
            })
            .map(|(series_id, _)| *series_id)
            .collect())
    }

    fn compaction_candidates(
        &self,
        cutoff_ts: i64,
        raw_encoding: u8,
        target_points: u32,
    ) -> Result<Vec<StoredChunk>, String> {
        let state = self.lock();
        let mut chunks: Vec<(i64, i64, i64, ChunkMeta)> = state
            .raw()
            .filter(|(_, row)| {
                row.max_ts < cutoff_ts
                    && (row.encoding == raw_encoding || row.point_count != target_points)
            })
            .map(|(rowid, row)| (row.series_id, row.min_ts, rowid, row.meta(rowid)))
            .collect();
        chunks.sort_by_key(|(series_id, min_ts, rowid, _)| (*series_id, *min_ts, *rowid));
        Ok(chunks
            .into_iter()
            .map(|(series_id, _, _, meta)| StoredChunk { series_id, meta })
            .collect())
    }

    fn chunks_present(&self, chunks: &[ChunkIdentity]) -> Result<bool, String> {
        let state = self.lock();
        for chunk in chunks {
            let present = state.rows.get(&row_id(&chunk.loc)?).is_some_and(|row| {
                row.series_id == chunk.series_id
                    && row.resolution == chunk.resolution
                    && row.min_ts == chunk.min_ts
                    && row.point_count == chunk.point_count
            });
            if !present {
                return Ok(false);
            }
        }
        Ok(true)
    }

    // ── Rollup chunk index ───────────────────────────────────────────

    fn put_rollup_chunks(&self, chunks: &[EncodedRollupChunk]) -> Result<Vec<ChunkLoc>, String> {
        let mut state = self.lock();
        Ok(chunks
            .iter()
            .map(|chunk| ChunkLoc::Row {
                rowid: state.insert_row(Row {
                    series_id: chunk.series_id,
                    resolution: chunk.resolution,
                    min_ts: chunk.min_ts,
                    max_ts: chunk.max_ts,
                    max_ts_val: None,
                    point_count: chunk.bucket_count,
                    min_val: 0.0,
                    max_val: 0.0,
                    sum_val: 0.0,
                    encoding: ENC_ROLLUP_V1,
                    ts: chunk.payload.clone(),
                    val: Vec::new(),
                }),
            })
            .collect())
    }

    fn scan_rollups(&self) -> Result<Vec<StoredRollupChunk>, String> {
        Ok(self
            .lock()
            .rollups()
            .map(|(rowid, row)| StoredRollupChunk {
                series_id: row.series_id,
                resolution: row.resolution,
                meta: row.meta(rowid),
            })
            .collect())
    }

    fn rollup_watermarks(
        &self,
        resolution: i64,
        series_ids: &[i64],
    ) -> Result<Vec<(i64, i64)>, String> {
        let state = self.lock();
        Ok(series_ids
            .iter()
            .filter_map(|series_id| {
                state
                    .rollups()
                    .filter(|(_, row)| row.series_id == *series_id && row.resolution == resolution)
                    .max_by_key(|(rowid, row)| (row.min_ts, *rowid))
                    .map(|(_, row)| (*series_id, row.max_ts))
            })
            .collect())
    }

    fn rollup_metas_for(
        &self,
        resolution: i64,
        series_ids: &[i64],
        start: i64,
        stop: i64,
    ) -> Result<Vec<StoredRollupChunk>, String> {
        let wanted: BTreeSet<i64> = series_ids.iter().copied().collect();
        let state = self.lock();
        let mut chunks: Vec<(i64, i64, i64, StoredRollupChunk)> = state
            .rollups()
            .filter(|(_, row)| {
                row.resolution == resolution
                    && wanted.contains(&row.series_id)
                    && row.min_ts <= stop
                    && row.max_ts >= start
            })
            .map(|(rowid, row)| {
                (
                    row.series_id,
                    row.min_ts,
                    rowid,
                    Self::rollup_chunk(rowid, row),
                )
            })
            .collect();
        chunks.sort_by_key(|(series_id, min_ts, rowid, _)| (*series_id, *min_ts, *rowid));
        Ok(chunks.into_iter().map(|(_, _, _, chunk)| chunk).collect())
    }

    fn visit_rollup_groups(
        &self,
        visitor: &mut dyn FnMut(StoredRollupChunk) -> Result<(), String>,
    ) -> Result<(), String> {
        let mut chunks: Vec<(i64, i64, i64, i64, StoredRollupChunk)> = self
            .lock()
            .rollups()
            .map(|(rowid, row)| {
                (
                    row.series_id,
                    row.resolution,
                    row.min_ts,
                    rowid,
                    Self::rollup_chunk(rowid, row),
                )
            })
            .collect();
        chunks.sort_by_key(|(series_id, resolution, min_ts, rowid, _)| {
            (*series_id, *resolution, *min_ts, *rowid)
        });
        for (_, _, _, _, chunk) in chunks {
            visitor(chunk)?;
        }
        Ok(())
    }

    fn rollup_victims(
        &self,
        resolution: Option<i64>,
        before: Option<i64>,
        after: Option<i64>,
        limit: usize,
    ) -> Result<Vec<(ChunkLoc, u32)>, String> {
        Ok(self
            .lock()
            .rollups()
            .filter(|(_, row)| {
                resolution.is_none_or(|resolution| row.resolution == resolution)
                    && before.is_none_or(|before| row.max_ts < before)
                    && after.is_none_or(|after| row.min_ts > after)
            })
            .take(limit)
            .map(|(rowid, row)| (ChunkLoc::Row { rowid }, row.point_count))
            .collect())
    }

    fn rollup_series_ids(&self) -> Result<Vec<i64>, String> {
        let ids: BTreeSet<i64> = self
            .lock()
            .rollups()
            .map(|(_, row)| row.series_id)
            .collect();
        Ok(ids.into_iter().collect())
    }

    fn rollup_resolutions(&self) -> Result<Vec<i64>, String> {
        let resolutions: BTreeSet<i64> = self
            .lock()
            .rollups()
            .map(|(_, row)| row.resolution)
            .collect();
        Ok(resolutions.into_iter().collect())
    }

    fn rollup_totals(&self) -> Result<(usize, usize), String> {
        let state = self.lock();
        Ok((
            state.rollups().count(),
            state
                .rollups()
                .filter(|(_, row)| row.point_count == 1)
                .count(),
        ))
    }

    // ── Change detection ─────────────────────────────────────────────

    fn catalog_generation(&self) -> Result<Option<(i64, i64)>, String> {
        let state = self.lock();
        Ok(Some((
            state.series.keys().next_back().copied().unwrap_or(0),
            state.generation,
        )))
    }

    fn append_watermark(&self) -> Result<Option<(i64, i64)>, String> {
        let state = self.lock();
        Ok(Some((
            state.shape_generation,
            state.rows.keys().next_back().copied().unwrap_or(0),
        )))
    }

    fn scan_since(
        &self,
        after_rowid: i64,
    ) -> Result<(Vec<StoredChunk>, Vec<StoredRollupChunk>), String> {
        let state = self.lock();
        let raw = state
            .raw()
            .filter(|(rowid, _)| *rowid > after_rowid)
            .map(|(rowid, row)| StoredChunk {
                series_id: row.series_id,
                meta: row.meta(rowid),
            })
            .collect();
        let rollups = state
            .rollups()
            .filter(|(rowid, _)| *rowid > after_rowid)
            .map(|(rowid, row)| StoredRollupChunk {
                series_id: row.series_id,
                resolution: row.resolution,
                meta: row.meta(rowid),
            })
            .collect();
        Ok((raw, rollups))
    }

    fn storage_stats(&self) -> (u64, usize) {
        let state = self.lock();
        (
            state
                .rows
                .values()
                .map(|row| (row.ts.len() + row.val.len()) as u64)
                .sum(),
            state.rows.len(),
        )
    }

    fn sweep_cache(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chunk(series_id: i64, min_ts: i64, points: u32) -> EncodedChunk {
        EncodedChunk {
            series_id,
            min_ts,
            max_ts: min_ts + i64::from(points) - 1,
            max_ts_val: 0.0,
            point_count: points,
            min_val: 0.0,
            max_val: 0.0,
            sum_val: 0.0,
            encoding: 1,
            ts_bytes: vec![0; 8],
            val_bytes: vec![0; 8],
        }
    }

    #[test]
    fn a_freed_row_id_goes_to_the_next_row_and_is_not_the_old_chunk() {
        let store = MemStore::new();
        let locs = store
            .put_chunks(&[chunk(1, 0, 4), chunk(1, 10, 4)])
            .unwrap();
        let planned = ChunkIdentity {
            loc: locs[1].clone(),
            series_id: 1,
            resolution: 0,
            min_ts: 10,
            point_count: 4,
        };
        assert!(store
            .chunks_present(std::slice::from_ref(&planned))
            .unwrap());
        assert!(store.delete_chunks(&locs[1..]).is_empty());
        let reused = store.put_chunks(&[chunk(2, 10, 4)]).unwrap();
        assert_eq!(reused[0], locs[1], "the freed id is given out again");
        assert!(!store.chunks_present(&[planned]).unwrap());
    }

    #[test]
    fn a_rollback_undoes_rows_and_series_and_clones_share_the_store() {
        let store = MemStore::new();
        let other = store.clone();
        let kept = store.resolve_series("cpu", &[]).unwrap();
        store.put_chunks(&[chunk(kept.id, 0, 1)]).unwrap();
        store.begin();
        let gone = store
            .resolve_series("mem", &[("host".into(), "a".into())])
            .unwrap();
        assert!(gone.created);
        store.put_chunks(&[chunk(gone.id, 0, 1)]).unwrap();
        store.begin();
        store.put_chunks(&[chunk(kept.id, 5, 1)]).unwrap();
        store.rollback();
        assert_eq!(other.row_count(), 2, "the savepoint's row went");
        store.rollback();
        assert_eq!(other.row_count(), 1);
        assert_eq!(other.metric_names().unwrap(), ["cpu"]);
        assert_eq!(other.chunk_series_ids().unwrap(), [kept.id]);
    }

    #[test]
    fn selectors_and_discovery_answer_from_the_catalog() {
        let store = MemStore::new();
        let label = |k: &str, v: &str| (k.to_owned(), v.to_owned());
        let a = store
            .resolve_series("cpu", &[label("host", "a"), label("env", "prod")])
            .unwrap()
            .id;
        let b = store
            .resolve_series("cpu", &[label("host", "b"), label("env", "dev")])
            .unwrap()
            .id;
        let m = store
            .resolve_series("mem", &[label("host", "a")])
            .unwrap()
            .id;
        assert_eq!(store.find_series_ids(Some("cpu"), &[]).unwrap(), [a, b]);
        assert_eq!(
            store.find_series_ids(None, &[label("host", "a")]).unwrap(),
            [a, m]
        );
        assert_eq!(
            store
                .find_series_ids_any(
                    Some("cpu"),
                    &[],
                    &[("env".to_owned(), vec!["dev".to_owned(), "qa".to_owned()])]
                )
                .unwrap(),
            [b]
        );
        assert_eq!(store.label_values(Some("cpu"), "host").unwrap(), ["a", "b"]);
        assert_eq!(store.label_values(None, NAME_KEY).unwrap(), ["cpu", "mem"]);
        assert_eq!(store.label_names(Some("mem")).unwrap(), [NAME_KEY, "host"]);
        assert!(store.label_names(Some("disk")).unwrap().is_empty());
        // The same identity, whatever order its labels are given in.
        let again = store
            .resolve_series("cpu", &[label("env", "prod"), label("host", "a")])
            .unwrap();
        assert_eq!((again.id, again.created), (a, false));
    }
}
