//! The on-disk series label index of a metrics table (#132).
//!
//! Two shadow tables beside `<t>_series`, which stays the source of truth:
//!
//! - `<t>_labels`: every distinct `(key, value)` pair once, metric names as
//!   `('__name__', name)`, with `series`, the length of its posting list,
//!   so a selector can drive from the shortest list.
//! - `<t>_postings`: one `(label_id, series_id)` row per label of a series,
//!   `WITHOUT ROWID`, so a posting list is one range of the primary key.
//!
//! Both are derived data and can always be rebuilt from `<t>_series`. Every
//! write happens in the caller's transaction, so a rollback reverts the
//! index with the series rows. Two `_meta` watermarks say whether the index
//! is current. `label_index_series_max`, the highest series id indexed,
//! catches series another writer (an older extension) inserted; they are
//! indexed incrementally. A deletion such a writer made shows as fewer
//! catalog rows than indexed series (every series has exactly one
//! `__name__` posting), and only a rebuild is then safe. Readers recheck
//! the labels of the rows they fetch, so a stale posting can make a result
//! miss a series but never include a wrong one.

use rusqlite::{params, params_from_iter, types::Value, Connection, OptionalExtension};
#[cfg(test)]
use std::collections::BTreeMap;
use std::collections::{BTreeSet, HashMap};

use crate::sql_ident;

/// A catalog row: id, metric name, sorted label pairs.
pub(crate) type SeriesRow = (i64, String, Vec<(String, String)>);
/// A series being indexed: id, metric name, sorted label pairs.
pub(crate) type NewSeries<'a> = (i64, &'a str, &'a [(String, String)]);

/// The label key metric names are indexed under.
pub(crate) const NAME_KEY: &str = "__name__";

/// Below this many series in a metric, a per-metric discovery read decodes
/// the metric's own series rows; above it, it probes posting lists.
const DECODE_METRIC_ROWS_BELOW: i64 = 20_000;

/// Rows per multi-row statement: far below SQLITE_MAX_VARIABLE_NUMBER.
const ROWS_PER_STATEMENT: usize = 4_000;

pub(crate) fn ddl(database: &str, table: &str) -> String {
    let labels = sql_ident::qualified_shadow(database, table, "labels");
    let postings = sql_ident::qualified_shadow(database, table, "postings");
    format!(
        r#"
CREATE TABLE IF NOT EXISTS {labels} (
  id     INTEGER PRIMARY KEY,
  key    TEXT NOT NULL,
  value  TEXT NOT NULL,
  series INTEGER NOT NULL DEFAULT 0,
  UNIQUE(key, value)
);
CREATE TABLE IF NOT EXISTS {postings} (
  label_id  INTEGER NOT NULL,
  series_id INTEGER NOT NULL,
  PRIMARY KEY(label_id, series_id)
) WITHOUT ROWID;
"#
    )
}

pub(crate) fn drop_ddl(database: &str, table: &str) -> String {
    let labels = sql_ident::qualified_shadow(database, table, "labels");
    let postings = sql_ident::qualified_shadow(database, table, "postings");
    format!("DROP TABLE IF EXISTS {postings};\nDROP TABLE IF EXISTS {labels};")
}

/// Decode `<t>_series.canonical_labels` (count, then length-prefixed key
/// and value, all big-endian u32).
pub(crate) fn decode_labels(data: &[u8]) -> Result<Vec<(String, String)>, String> {
    let mut pos = 0usize;
    let take_u32 = |pos: &mut usize| -> Result<usize, String> {
        let end = pos.checked_add(4).ok_or("label catalog overflow")?;
        let bytes: [u8; 4] = data
            .get(*pos..end)
            .ok_or("truncated label catalog")?
            .try_into()
            .map_err(|_| "truncated label catalog")?;
        *pos = end;
        Ok(u32::from_be_bytes(bytes) as usize)
    };
    let count = take_u32(&mut pos)?;
    let mut out = Vec::with_capacity(count.min(64));
    for _ in 0..count {
        let text = |pos: &mut usize| -> Result<String, String> {
            let len = take_u32(pos)?;
            let end = pos.checked_add(len).ok_or("label catalog overflow")?;
            let bytes = data.get(*pos..end).ok_or("truncated label catalog")?;
            *pos = end;
            String::from_utf8(bytes.to_vec()).map_err(|_| "label catalog is not UTF-8".to_string())
        };
        let key = text(&mut pos)?;
        let value = text(&mut pos)?;
        out.push((key, value));
    }
    Ok(out)
}

/// Pre-formatted SQL for one metrics table's label index.
pub(crate) struct LabelIndex {
    labels: String,
    postings: String,
    series: String,
    meta: String,
    probe_sql: String,
    label_sql: String,
    posting_sql: String,
    series_rows_prefix: String,
}

impl LabelIndex {
    pub(crate) fn new(database: &str, table: &str) -> Self {
        let labels = sql_ident::qualified_shadow(database, table, "labels");
        let postings = sql_ident::qualified_shadow(database, table, "postings");
        let series = sql_ident::qualified_shadow(database, table, "series");
        let meta = sql_ident::qualified_shadow(database, table, "meta");
        LabelIndex {
            probe_sql: format!("SELECT 1 FROM {labels} LIMIT 0"),
            label_sql: format!("SELECT id, series FROM {labels} WHERE key = ?1 AND value = ?2"),
            posting_sql: format!(
                "SELECT series_id FROM {postings} WHERE label_id = ?1 ORDER BY series_id"
            ),
            series_rows_prefix: format!(
                "SELECT id, name, canonical_labels FROM {series} WHERE id IN ("
            ),
            labels,
            postings,
            series,
            meta,
        }
    }

    /// Whether this table has the label index tables at all. Tables created
    /// before #132 have them only after an explicit upgrade.
    pub(crate) fn present(&self, conn: &Connection) -> bool {
        conn.prepare_cached(&self.probe_sql).is_ok()
    }

    fn meta_i64(&self, conn: &Connection, key: &str) -> Result<i64, String> {
        conn.query_row(
            &format!(
                "SELECT COALESCE((SELECT CAST(v AS INTEGER) FROM {} WHERE k = ?1), 0)",
                self.meta
            ),
            [key],
            |row| row.get(0),
        )
        .map_err(|e| format!("read {key}: {e}"))
    }

    fn set_meta(&self, conn: &Connection, key: &str, value: i64) -> Result<(), String> {
        conn.execute(
            &format!(
                "INSERT INTO {} (k, v) VALUES (?1, ?2) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
                self.meta
            ),
            params![key, value],
        )
        .map(|_| ())
        .map_err(|e| format!("write {key}: {e}"))
    }

    fn series_max(&self, conn: &Connection) -> Result<i64, String> {
        conn.query_row(
            &format!("SELECT COALESCE(MAX(id), 0) FROM {}", self.series),
            [],
            |row| row.get(0),
        )
        .map_err(|e| format!("read series high water: {e}"))
    }

    /// Index series rows just inserted into `<t>_series`, in the caller's
    /// transaction: label rows upserted once per distinct pair, postings in
    /// multi-row statements, and posting-list lengths moved by the number
    /// of postings actually added.
    pub(crate) fn index_series(
        &self,
        conn: &Connection,
        rows: &[NewSeries<'_>],
    ) -> Result<(), String> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut pairs: BTreeSet<(&str, &str)> = BTreeSet::new();
        for (_, name, labels) in rows {
            pairs.insert((NAME_KEY, name));
            for (key, value) in labels.iter() {
                pairs.insert((key, value));
            }
        }
        let ids = self.label_ids(conn, &pairs, true)?;
        let mut postings: Vec<(i64, i64)> = Vec::new();
        for (series_id, name, labels) in rows {
            postings.push((ids[&(NAME_KEY, *name)], *series_id));
            for (key, value) in labels.iter() {
                postings.push((ids[&(key.as_str(), value.as_str())], *series_id));
            }
        }
        let mut added: HashMap<i64, i64> = HashMap::new();
        for batch in postings.chunks(ROWS_PER_STATEMENT) {
            let mut sql = format!(
                "INSERT INTO {} (label_id, series_id) VALUES ",
                self.postings
            );
            sql.push_str(&vec!["(?,?)"; batch.len()].join(","));
            sql.push_str(" ON CONFLICT DO NOTHING RETURNING label_id");
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| format!("prepare posting insert: {e}"))?;
            let binds = batch.iter().flat_map(|(label, series)| [*label, *series]);
            let mut inserted = stmt
                .query(params_from_iter(binds))
                .map_err(|e| format!("posting insert: {e}"))?;
            while let Some(row) = inserted
                .next()
                .map_err(|e| format!("posting insert: {e}"))?
            {
                *added
                    .entry(row.get(0).map_err(|e| format!("posting insert: {e}"))?)
                    .or_default() += 1;
            }
        }
        self.move_counts(conn, &added)?;
        let high = rows.iter().map(|(id, ..)| *id).max().unwrap_or(0);
        if high > self.meta_i64(conn, "label_index_series_max")? {
            self.set_meta(conn, "label_index_series_max", high)?;
        }
        Ok(())
    }

    /// Index one series with single-row statements only. The engine's
    /// single-series resolve calls the store while holding its journal
    /// lock; a multi-row statement there would open a statement journal,
    /// re-enter the vtab's savepoint hook, and deadlock on that lock (the
    /// hazard `Engine::resolve_series_batch` documents). The batch path
    /// holds no engine lock and may use [`Self::index_series`].
    pub(crate) fn index_one(
        &self,
        conn: &Connection,
        series_id: i64,
        name: &str,
        labels: &[(String, String)],
    ) -> Result<(), String> {
        let mut upsert = conn
            .prepare_cached(&format!(
                "INSERT INTO {} (key, value) VALUES (?1, ?2) ON CONFLICT(key, value) DO NOTHING",
                self.labels
            ))
            .map_err(|e| format!("prepare label insert: {e}"))?;
        let mut post = conn
            .prepare_cached(&format!(
                "INSERT INTO {} (label_id, series_id) VALUES (?1, ?2) ON CONFLICT DO NOTHING",
                self.postings
            ))
            .map_err(|e| format!("prepare posting insert: {e}"))?;
        let mut count = conn
            .prepare_cached(&format!(
                "UPDATE {} SET series = series + 1 WHERE id = ?1",
                self.labels
            ))
            .map_err(|e| format!("prepare label count update: {e}"))?;
        let pairs = std::iter::once((NAME_KEY, name))
            .chain(labels.iter().map(|(k, v)| (k.as_str(), v.as_str())));
        for (key, value) in pairs {
            upsert
                .execute([key, value])
                .map_err(|e| format!("label insert: {e}"))?;
            let (label, _) = self
                .label(conn, key, value)?
                .ok_or_else(|| format!("label ({key}, {value}) missing after insert"))?;
            if post
                .execute(params![label, series_id])
                .map_err(|e| format!("posting insert: {e}"))?
                > 0
            {
                count
                    .execute([label])
                    .map_err(|e| format!("label count update: {e}"))?;
            }
        }
        if series_id > self.meta_i64(conn, "label_index_series_max")? {
            self.set_meta(conn, "label_index_series_max", series_id)?;
        }
        Ok(())
    }

    /// Unindex series about to be deleted from `<t>_series`, in the caller's
    /// transaction. Reads the rows first, so each posting is removed by its
    /// primary key, and labels no series carries any more are removed too.
    pub(crate) fn unindex_series(&self, conn: &Connection, ids: &[i64]) -> Result<(), String> {
        let rows = self.series_rows(conn, ids)?;
        let mut pairs: BTreeSet<(&str, &str)> = BTreeSet::new();
        for (_, name, labels) in &rows {
            pairs.insert((NAME_KEY, name.as_str()));
            for (key, value) in labels {
                pairs.insert((key, value));
            }
        }
        let label_ids = self.label_ids(conn, &pairs, false)?;
        let mut removed: HashMap<i64, i64> = HashMap::new();
        let mut delete = conn
            .prepare_cached(&format!(
                "DELETE FROM {} WHERE label_id = ?1 AND series_id = ?2",
                self.postings
            ))
            .map_err(|e| format!("prepare posting delete: {e}"))?;
        for (series_id, name, labels) in &rows {
            let keys = std::iter::once((NAME_KEY, name.as_str()))
                .chain(labels.iter().map(|(k, v)| (k.as_str(), v.as_str())));
            for pair in keys {
                if let Some(&label) = label_ids.get(&pair) {
                    if delete
                        .execute(params![label, series_id])
                        .map_err(|e| format!("posting delete: {e}"))?
                        > 0
                    {
                        *removed.entry(label).or_default() -= 1;
                    }
                }
            }
        }
        self.move_counts(conn, &removed)?;
        conn.execute(
            &format!("DELETE FROM {} WHERE series <= 0", self.labels),
            [],
        )
        .map_err(|e| format!("remove empty labels: {e}"))?;
        Ok(())
    }

    fn move_counts(&self, conn: &Connection, deltas: &HashMap<i64, i64>) -> Result<(), String> {
        let mut update = conn
            .prepare_cached(&format!(
                "UPDATE {} SET series = series + ?2 WHERE id = ?1",
                self.labels
            ))
            .map_err(|e| format!("prepare label count update: {e}"))?;
        for (label, delta) in deltas {
            update
                .execute(params![label, delta])
                .map_err(|e| format!("label count update: {e}"))?;
        }
        Ok(())
    }

    /// Ids of `pairs`, creating missing labels when `create`.
    fn label_ids<'a>(
        &self,
        conn: &Connection,
        pairs: &BTreeSet<(&'a str, &'a str)>,
        create: bool,
    ) -> Result<HashMap<(&'a str, &'a str), i64>, String> {
        let pairs: Vec<(&str, &str)> = pairs.iter().copied().collect();
        let mut out = HashMap::with_capacity(pairs.len());
        for batch in pairs.chunks(ROWS_PER_STATEMENT / 2) {
            if create {
                let mut sql = format!("INSERT INTO {} (key, value) VALUES ", self.labels);
                sql.push_str(&vec!["(?,?)"; batch.len()].join(","));
                sql.push_str(" ON CONFLICT(key, value) DO NOTHING");
                conn.execute(
                    &sql,
                    params_from_iter(batch.iter().flat_map(|(k, v)| [*k, *v])),
                )
                .map_err(|e| format!("label insert: {e}"))?;
            }
            let mut sql = String::from("WITH req(ord, key, value) AS (VALUES ");
            for i in 0..batch.len() {
                if i > 0 {
                    sql.push(',');
                }
                sql.push_str(&format!("({i},?,?)"));
            }
            sql.push_str(&format!(
                ") SELECT req.ord, l.id FROM req JOIN {} l ON l.key = req.key AND l.value = req.value",
                self.labels
            ));
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| format!("prepare label lookup: {e}"))?;
            let found: Vec<(usize, i64)> = stmt
                .query_map(
                    params_from_iter(batch.iter().flat_map(|(k, v)| [*k, *v])),
                    |r| Ok((r.get::<_, i64>(0)? as usize, r.get(1)?)),
                )
                .map_err(|e| format!("label lookup: {e}"))?
                .collect::<Result<_, _>>()
                .map_err(|e| format!("label lookup: {e}"))?;
            for (ord, id) in found {
                out.insert(batch[ord], id);
            }
        }
        if create && out.len() != pairs.len() {
            return Err(format!(
                "label upsert resolved {} of {} pairs",
                out.len(),
                pairs.len()
            ));
        }
        Ok(out)
    }

    /// `(id, name, labels)` for `ids`, in id order; absent ids are skipped.
    pub(crate) fn series_rows(
        &self,
        conn: &Connection,
        ids: &[i64],
    ) -> Result<Vec<SeriesRow>, String> {
        let mut out = Vec::with_capacity(ids.len());
        for batch in ids.chunks(ROWS_PER_STATEMENT) {
            let sql = format!(
                "{}{}) ORDER BY id",
                self.series_rows_prefix,
                vec!["?"; batch.len()].join(",")
            );
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| format!("prepare series rows: {e}"))?;
            let mut rows = stmt
                .query(params_from_iter(batch.iter()))
                .map_err(|e| format!("series rows: {e}"))?;
            while let Some(row) = rows.next().map_err(|e| format!("series rows: {e}"))? {
                let blob: Vec<u8> = row.get(2).map_err(|e| format!("series rows: {e}"))?;
                out.push((
                    row.get(0).map_err(|e| format!("series rows: {e}"))?,
                    row.get(1).map_err(|e| format!("series rows: {e}"))?,
                    decode_labels(&blob)?,
                ));
            }
        }
        out.sort_unstable_by_key(|(id, ..)| *id);
        Ok(out)
    }

    fn catalog_rows(&self, conn: &Connection) -> Result<i64, String> {
        conn.query_row(&format!("SELECT count(*) FROM {}", self.series), [], |r| {
            r.get(0)
        })
        .map_err(|e| format!("count series: {e}"))
    }

    /// Series the index holds: one `__name__` posting each.
    fn indexed_rows(&self, conn: &Connection) -> Result<i64, String> {
        conn.query_row(
            &format!(
                "SELECT COALESCE(SUM(series), 0) FROM {} WHERE key = ?1",
                self.labels
            ),
            [NAME_KEY],
            |r| r.get(0),
        )
        .map_err(|e| format!("count indexed series: {e}"))
    }

    /// Whether the index reflects `<t>_series` as it now is.
    pub(crate) fn is_current(&self, conn: &Connection) -> Result<bool, String> {
        Ok(self.present(conn)
            && self.meta_i64(conn, "label_index_series_max")? == self.series_max(conn)?
            && self.indexed_rows(conn)? == self.catalog_rows(conn)?)
    }

    /// Bring the index up to date: index series inserted past the
    /// watermark; then, if it still holds a different number of series than
    /// the catalog (a deletion it did not see), rebuild it. Creates the
    /// tables if missing. Runs in the caller's transaction.
    pub(crate) fn make_current(
        &self,
        conn: &Connection,
        database: &str,
        table: &str,
    ) -> Result<(), String> {
        if !self.present(conn) {
            conn.execute_batch(&ddl(database, table))
                .map_err(|e| format!("create label index: {e}"))?;
        }
        if self.is_current(conn)? {
            return Ok(());
        }
        let watermark = self.meta_i64(conn, "label_index_series_max")?;
        self.backfill(conn, watermark)?;
        if self.indexed_rows(conn)? != self.catalog_rows(conn)? {
            conn.execute_batch(&format!(
                "DELETE FROM {}; DELETE FROM {};",
                self.postings, self.labels
            ))
            .map_err(|e| format!("clear label index: {e}"))?;
            self.backfill(conn, 0)?;
        }
        let high = self.series_max(conn)?;
        self.set_meta(conn, "label_index_series_max", high)
    }

    fn backfill(&self, conn: &Connection, after: i64) -> Result<(), String> {
        let mut select = conn
            .prepare(&format!(
                "SELECT id, name, canonical_labels FROM {} WHERE id > ?1 ORDER BY id",
                self.series
            ))
            .map_err(|e| format!("prepare label index backfill: {e}"))?;
        let mut rows = select
            .query([after])
            .map_err(|e| format!("label index backfill: {e}"))?;
        let mut batch: Vec<SeriesRow> = Vec::new();
        let flush = |batch: &mut Vec<SeriesRow>| -> Result<(), String> {
            let view: Vec<NewSeries<'_>> = batch
                .iter()
                .map(|(id, name, labels)| (*id, name.as_str(), labels.as_slice()))
                .collect();
            self.index_series(conn, &view)?;
            batch.clear();
            Ok(())
        };
        while let Some(row) = rows
            .next()
            .map_err(|e| format!("label index backfill: {e}"))?
        {
            let blob: Vec<u8> = row
                .get(2)
                .map_err(|e| format!("label index backfill: {e}"))?;
            batch.push((
                row.get(0)
                    .map_err(|e| format!("label index backfill: {e}"))?,
                row.get(1)
                    .map_err(|e| format!("label index backfill: {e}"))?,
                decode_labels(&blob)?,
            ));
            if batch.len() == 2_000 {
                flush(&mut batch)?;
            }
        }
        flush(&mut batch)
    }

    fn label(
        &self,
        conn: &Connection,
        key: &str,
        value: &str,
    ) -> Result<Option<(i64, i64)>, String> {
        conn.prepare_cached(&self.label_sql)
            .map_err(|e| format!("prepare label lookup: {e}"))?
            .query_row([key, value], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()
            .map_err(|e| format!("label lookup: {e}"))
    }

    fn posting(&self, conn: &Connection, label: i64) -> Result<Vec<i64>, String> {
        conn.prepare_cached(&self.posting_sql)
            .map_err(|e| format!("prepare posting read: {e}"))?
            .query_map([label], |r| r.get(0))
            .map_err(|e| format!("posting read: {e}"))?
            .collect::<Result<_, _>>()
            .map_err(|e| format!("posting read: {e}"))
    }

    /// Series of `metric` carrying every `(key, value)` in `eq`, ascending.
    /// Only the shortest posting list is scanned; every other condition is
    /// a primary-key probe per candidate, as `find_series` does in memory.
    pub(crate) fn find_series(
        &self,
        conn: &Connection,
        metric: Option<&str>,
        eq: &[(String, String)],
    ) -> Result<Vec<i64>, String> {
        let mut required: Vec<(i64, i64)> = Vec::with_capacity(eq.len() + 1);
        for (key, value) in metric
            .map(|m| (NAME_KEY, m))
            .into_iter()
            .chain(eq.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        {
            match self.label(conn, key, value)? {
                Some((id, series)) => required.push((series, id)),
                None => return Ok(Vec::new()),
            }
        }
        if required.is_empty() {
            return Err("find_series needs a metric or at least one label".into());
        }
        required.sort_unstable();
        let (_, driver) = required[0];
        if required.len() == 1 {
            return self.posting(conn, driver);
        }
        let mut sql = format!(
            "SELECT a.series_id FROM {} a WHERE a.label_id = ?",
            self.postings
        );
        for _ in &required[1..] {
            sql.push_str(&format!(
                " AND EXISTS (SELECT 1 FROM {} b WHERE b.label_id = ? AND b.series_id = a.series_id)",
                self.postings
            ));
        }
        sql.push_str(" ORDER BY a.series_id");
        let binds: Vec<Value> = std::iter::once(driver)
            .chain(required[1..].iter().map(|(_, id)| *id))
            .map(Value::Integer)
            .collect();
        conn.prepare_cached(&sql)
            .map_err(|e| format!("prepare series selection: {e}"))?
            .query_map(params_from_iter(binds), |r| r.get(0))
            .map_err(|e| format!("series selection: {e}"))?
            .collect::<Result<_, _>>()
            .map_err(|e| format!("series selection: {e}"))
    }

    /// Distinct metric names, sorted.
    pub(crate) fn metric_names(&self, conn: &Connection) -> Result<Vec<String>, String> {
        self.values_of_key(conn, NAME_KEY)
    }

    fn values_of_key(&self, conn: &Connection, key: &str) -> Result<Vec<String>, String> {
        conn.prepare_cached(&format!(
            "SELECT value FROM {} WHERE key = ?1 AND series > 0 ORDER BY value",
            self.labels
        ))
        .map_err(|e| format!("prepare label values: {e}"))?
        .query_map([key], |r| r.get(0))
        .map_err(|e| format!("label values: {e}"))?
        .collect::<Result<_, _>>()
        .map_err(|e| format!("label values: {e}"))
    }

    /// Values `key` takes, across every series or within `metric`, sorted.
    pub(crate) fn label_values(
        &self,
        conn: &Connection,
        metric: Option<&str>,
        key: &str,
    ) -> Result<Vec<String>, String> {
        let Some(metric) = metric else {
            return self.values_of_key(conn, key);
        };
        if key == NAME_KEY {
            return Ok(self
                .label(conn, NAME_KEY, metric)?
                .filter(|(_, series)| *series > 0)
                .map(|_| vec![metric.to_owned()])
                .unwrap_or_default());
        }
        let Some((name_label, series)) = self.label(conn, NAME_KEY, metric)? else {
            return Ok(Vec::new());
        };
        if series < DECODE_METRIC_ROWS_BELOW {
            let ids = self.posting(conn, name_label)?;
            let values: BTreeSet<String> = self
                .series_rows(conn, &ids)?
                .into_iter()
                .flat_map(|(_, _, labels)| labels)
                .filter(|(k, _)| k == key)
                .map(|(_, v)| v)
                .collect();
            return Ok(values.into_iter().collect());
        }
        conn.prepare_cached(&format!(
            "SELECT l.value FROM {labels} l WHERE l.key = ?1 AND l.series > 0 AND EXISTS ( \
               SELECT 1 FROM {postings} p WHERE p.label_id = l.id AND EXISTS ( \
                 SELECT 1 FROM {postings} q WHERE q.label_id = ?2 AND q.series_id = p.series_id)) \
             ORDER BY l.value",
            labels = self.labels,
            postings = self.postings
        ))
        .map_err(|e| format!("prepare metric label values: {e}"))?
        .query_map(params![key, name_label], |r| r.get(0))
        .map_err(|e| format!("metric label values: {e}"))?
        .collect::<Result<_, _>>()
        .map_err(|e| format!("metric label values: {e}"))
    }

    /// Label names, across every series or within `metric`, sorted; both
    /// include `__name__`.
    pub(crate) fn label_names(
        &self,
        conn: &Connection,
        metric: Option<&str>,
    ) -> Result<Vec<String>, String> {
        let Some(metric) = metric else {
            return conn
                .prepare_cached(&format!(
                    "SELECT DISTINCT key FROM {} WHERE series > 0 ORDER BY key",
                    self.labels
                ))
                .map_err(|e| format!("prepare label names: {e}"))?
                .query_map([], |r| r.get(0))
                .map_err(|e| format!("label names: {e}"))?
                .collect::<Result<_, _>>()
                .map_err(|e| format!("label names: {e}"));
        };
        let Some((name_label, _)) = self.label(conn, NAME_KEY, metric)? else {
            return Ok(Vec::new());
        };
        let ids = self.posting(conn, name_label)?;
        let mut names: BTreeSet<String> = BTreeSet::from([NAME_KEY.to_owned()]);
        for (_, _, labels) in self.series_rows(conn, &ids)? {
            names.extend(labels.into_iter().map(|(k, _)| k));
        }
        Ok(names.into_iter().collect())
    }

    /// Posting-list lengths by label, for tests and statistics.
    #[cfg(test)]
    pub(crate) fn counts(&self, conn: &Connection) -> BTreeMap<(String, String), i64> {
        conn.prepare(&format!("SELECT key, value, series FROM {}", self.labels))
            .unwrap()
            .query_map([], |r| Ok(((r.get(0)?, r.get(1)?), r.get(2)?)))
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(pairs: &[(&str, &str)]) -> Vec<u8> {
        let mut out = (pairs.len() as u32).to_be_bytes().to_vec();
        for (k, v) in pairs {
            for t in [k, v] {
                out.extend_from_slice(&(t.len() as u32).to_be_bytes());
                out.extend_from_slice(t.as_bytes());
            }
        }
        out
    }

    fn store() -> (Connection, LabelIndex) {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE m_series (id INTEGER PRIMARY KEY, name TEXT NOT NULL,
               canonical_labels BLOB NOT NULL, UNIQUE(name, canonical_labels));
             CREATE TABLE m_meta (k TEXT PRIMARY KEY, v BLOB);",
        )
        .unwrap();
        conn.execute_batch(&ddl("main", "m")).unwrap();
        (conn, LabelIndex::new("main", "m"))
    }

    fn add(conn: &Connection, index: &LabelIndex, name: &str, pairs: &[(&str, &str)]) -> i64 {
        conn.execute(
            "INSERT INTO m_series(name, canonical_labels) VALUES (?1, ?2)",
            params![name, encode(pairs)],
        )
        .unwrap();
        let id = conn.last_insert_rowid();
        let owned: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        index.index_series(conn, &[(id, name, &owned)]).unwrap();
        id
    }

    #[test]
    fn selects_drive_from_the_shortest_list_and_discovery_reads_labels() {
        let (conn, index) = store();
        let a = add(&conn, &index, "cpu", &[("host", "a"), ("env", "prod")]);
        let b = add(&conn, &index, "cpu", &[("host", "b"), ("env", "prod")]);
        let c = add(&conn, &index, "mem", &[("host", "a"), ("env", "dev")]);
        let eq = |k: &str, v: &str| vec![(k.to_string(), v.to_string())];
        assert_eq!(index.find_series(&conn, Some("cpu"), &[]).unwrap(), [a, b]);
        assert_eq!(
            index
                .find_series(&conn, Some("cpu"), &eq("host", "a"))
                .unwrap(),
            [a]
        );
        assert_eq!(
            index.find_series(&conn, None, &eq("host", "a")).unwrap(),
            [a, c]
        );
        assert!(index
            .find_series(&conn, Some("cpu"), &eq("host", "z"))
            .unwrap()
            .is_empty());
        assert!(index
            .find_series(&conn, Some("disk"), &[])
            .unwrap()
            .is_empty());
        assert_eq!(index.metric_names(&conn).unwrap(), ["cpu", "mem"]);
        assert_eq!(
            index.label_values(&conn, None, "env").unwrap(),
            ["dev", "prod"]
        );
        assert_eq!(
            index.label_values(&conn, Some("cpu"), "host").unwrap(),
            ["a", "b"]
        );
        assert_eq!(
            index.label_names(&conn, None).unwrap(),
            ["__name__", "env", "host"]
        );
        assert_eq!(
            index.label_names(&conn, Some("mem")).unwrap(),
            ["__name__", "env", "host"]
        );
        let counts = index.counts(&conn);
        assert_eq!(counts[&("host".into(), "a".into())], 2);
        assert_eq!(counts[&("__name__".into(), "cpu".into())], 2);
        assert!(index.is_current(&conn).unwrap());
    }

    #[test]
    fn unindexing_removes_postings_and_empty_labels_exactly() {
        let (conn, index) = store();
        let a = add(&conn, &index, "cpu", &[("host", "a")]);
        let b = add(&conn, &index, "cpu", &[("host", "b")]);
        index.unindex_series(&conn, &[a]).unwrap();
        conn.execute("DELETE FROM m_series WHERE id = ?1", [a])
            .unwrap();
        assert_eq!(index.find_series(&conn, Some("cpu"), &[]).unwrap(), [b]);
        assert_eq!(index.label_values(&conn, None, "host").unwrap(), ["b"]);
        let counts = index.counts(&conn);
        assert_eq!(counts[&("__name__".into(), "cpu".into())], 1);
        assert!(!counts.contains_key(&("host".into(), "a".into())));
    }

    #[test]
    fn make_current_catches_up_inserts_and_rebuilds_after_an_unseen_delete() {
        let (conn, index) = store();
        let a = add(&conn, &index, "cpu", &[("host", "a")]);
        // Another writer inserts without indexing.
        conn.execute(
            "INSERT INTO m_series(name, canonical_labels) VALUES ('cpu', ?1)",
            [encode(&[("host", "b")])],
        )
        .unwrap();
        let b = conn.last_insert_rowid();
        assert!(!index.is_current(&conn).unwrap());
        index.make_current(&conn, "main", "m").unwrap();
        assert_eq!(index.find_series(&conn, Some("cpu"), &[]).unwrap(), [a, b]);
        assert!(index.is_current(&conn).unwrap());
        // Another writer deletes without unindexing: the index holds more
        // series than the catalog, and only a rebuild is safe.
        conn.execute("DELETE FROM m_series WHERE id = ?1", [a])
            .unwrap();
        assert!(!index.is_current(&conn).unwrap());
        index.make_current(&conn, "main", "m").unwrap();
        assert_eq!(index.find_series(&conn, Some("cpu"), &[]).unwrap(), [b]);
        assert_eq!(index.label_values(&conn, None, "host").unwrap(), ["b"]);
        assert_eq!(index.counts(&conn)[&("__name__".into(), "cpu".into())], 1);
    }

    #[test]
    fn a_batch_upserts_shared_labels_once_and_counts_every_posting() {
        let (conn, index) = store();
        let mut rows = Vec::new();
        for i in 0..5_000 {
            let pairs = vec![
                ("env".to_string(), "prod".to_string()),
                ("host".to_string(), format!("h{}", i % 100)),
                ("i".to_string(), i.to_string()),
            ];
            let borrowed: Vec<(&str, &str)> = pairs
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            conn.execute(
                "INSERT INTO m_series(name, canonical_labels) VALUES ('cpu', ?1)",
                [encode(&borrowed)],
            )
            .unwrap();
            rows.push((conn.last_insert_rowid(), pairs));
        }
        let view: Vec<NewSeries<'_>> = rows
            .iter()
            .map(|(id, pairs)| (*id, "cpu", pairs.as_slice()))
            .collect();
        index.index_series(&conn, &view).unwrap();
        let counts = index.counts(&conn);
        assert_eq!(counts[&("env".into(), "prod".into())], 5_000);
        assert_eq!(counts[&("host".into(), "h7".into())], 50);
        // __name__, env, 100 hosts, 5,000 ids.
        assert_eq!(counts.len(), 5_102);
        assert_eq!(
            index
                .find_series(&conn, Some("cpu"), &[("host".into(), "h7".into())])
                .unwrap()
                .len(),
            50
        );
    }
}
