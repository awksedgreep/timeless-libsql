//! The on-disk series index of #132, prototyped over a store the
//! `--index-cache` harness built (phase 0, part 2).
//!
//! Runs in a fresh process on a plain SQLite connection, no extension and no
//! engine, so its resident memory is the disk design's: SQLite's page cache
//! plus the bounded resolve cache. It measures:
//! - the backfill of `_labels` / `_postings` / `_series_hash` from the
//!   durable `metrics_series` catalog, and the bytes it adds;
//! - a full ingest cycle's series resolution through a bounded cache, for two
//!   eviction policies and two miss paths, at budgets of the working set;
//! - selectors and discovery as SQL over the postings.
//!
//!   query-read --index-disk DB SERIES RUNS

use super::index_cache::{mac_pub, series_pairs, METRICS_PUB, SERIES_PER_GATEWAY_PUB};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Instant;

fn status_kib(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .unwrap_or_default()
        .lines()
        .find_map(|line| {
            line.strip_prefix(field)?
                .trim()
                .trim_end_matches("kB")
                .trim()
                .parse()
                .ok()
        })
        .unwrap_or(0)
}

/// `metrics_series.canonical_labels`, as the shadow store encodes it.
fn encode_labels<K: AsRef<str>, V: AsRef<str>>(pairs: &[(K, V)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(pairs.len() as u32).to_be_bytes());
    for (key, value) in pairs {
        for text in [key.as_ref(), value.as_ref()] {
            out.extend_from_slice(&(text.len() as u32).to_be_bytes());
            out.extend_from_slice(text.as_bytes());
        }
    }
    out
}

fn decode_labels(data: &[u8]) -> Vec<(String, String)> {
    let word = |at: usize| u32::from_be_bytes(data[at..at + 4].try_into().unwrap()) as usize;
    let count = word(0);
    let mut pos = 4;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let mut take = || {
            let len = word(pos);
            let text = String::from_utf8(data[pos + 4..pos + 4 + len].to_vec()).unwrap();
            pos += 4 + len;
            text
        };
        let key = take();
        let value = take();
        out.push((key, value));
    }
    out
}

/// The engine's identity hash shape: SipHash over the name and sorted pairs.
fn identity<K: AsRef<str>, V: AsRef<str>>(metric: &str, pairs: &[(K, V)]) -> u64 {
    let mut hasher = DefaultHasher::new();
    metric.hash(&mut hasher);
    for (key, value) in pairs {
        key.as_ref().hash(&mut hasher);
        value.as_ref().hash(&mut hasher);
    }
    hasher.finish()
}

/// The resolve cache, hash → id, under one of two policies.
enum Cache {
    /// CLOCK (second chance): an LRU approximation. A cyclic scan larger than
    /// its capacity evicts every entry before it is reused.
    Clock {
        slots: Vec<(u64, i64, bool)>,
        map: HashMap<u64, usize>,
        hand: usize,
        capacity: usize,
    },
    /// Admit until full, never evict: keeps a fixed subset, so a cyclic scan
    /// hits capacity / working set, the best any policy can do for it.
    Pinned {
        map: HashMap<u64, i64>,
        capacity: usize,
    },
}

impl Cache {
    fn get(&mut self, hash: u64) -> Option<i64> {
        match self {
            Cache::Clock { slots, map, .. } => map.get(&hash).map(|&slot| {
                slots[slot].2 = true;
                slots[slot].1
            }),
            Cache::Pinned { map, .. } => map.get(&hash).copied(),
        }
    }

    fn put(&mut self, hash: u64, id: i64) {
        match self {
            Cache::Clock {
                slots,
                map,
                hand,
                capacity,
            } => {
                if *capacity == 0 {
                    return;
                }
                if slots.len() < *capacity {
                    map.insert(hash, slots.len());
                    slots.push((hash, id, false));
                    return;
                }
                loop {
                    let slot = &mut slots[*hand];
                    if slot.2 {
                        slot.2 = false;
                        *hand = (*hand + 1) % *capacity;
                        continue;
                    }
                    map.remove(&slot.0);
                    *slot = (hash, id, false);
                    map.insert(hash, *hand);
                    *hand = (*hand + 1) % *capacity;
                    return;
                }
            }
            Cache::Pinned { map, capacity } => {
                if map.len() < *capacity {
                    map.insert(hash, id);
                }
            }
        }
    }
}

fn backfill(conn: &Connection) -> (f64, u64) {
    let before = page_bytes(conn);
    let started = Instant::now();
    conn.execute_batch(
        "BEGIN;
         CREATE TABLE metrics_labels (
           id INTEGER PRIMARY KEY, key TEXT NOT NULL, value TEXT NOT NULL,
           series INTEGER NOT NULL DEFAULT 0, UNIQUE(key, value));
         CREATE TABLE metrics_postings (
           label_id INTEGER NOT NULL, series_id INTEGER NOT NULL,
           PRIMARY KEY(label_id, series_id)) WITHOUT ROWID;
         CREATE TABLE metrics_series_hash (
           hash INTEGER PRIMARY KEY, series_id INTEGER NOT NULL);",
    )
    .unwrap();
    {
        let mut read = conn
            .prepare("SELECT id, name, canonical_labels FROM metrics_series ORDER BY id")
            .unwrap();
        let mut find = conn
            .prepare_cached("SELECT id FROM metrics_labels WHERE key = ?1 AND value = ?2")
            .unwrap();
        let mut add = conn
            .prepare_cached("INSERT INTO metrics_labels(key, value) VALUES (?1, ?2)")
            .unwrap();
        let mut post = conn
            .prepare_cached("INSERT INTO metrics_postings(label_id, series_id) VALUES (?1, ?2)")
            .unwrap();
        let mut hashed = conn
            .prepare_cached("INSERT INTO metrics_series_hash(hash, series_id) VALUES (?1, ?2)")
            .unwrap();
        // Label ids are interned in memory for the backfill only, as a
        // migration would; the steady state never holds this map.
        let mut interned: HashMap<(String, String), i64> = HashMap::new();
        let mut rows = read.query([]).unwrap();
        while let Some(row) = rows.next().unwrap() {
            let id: i64 = row.get(0).unwrap();
            let name: String = row.get(1).unwrap();
            let blob: Vec<u8> = row.get(2).unwrap();
            let pairs = decode_labels(&blob);
            hashed
                .execute(params![identity(&name, &pairs) as i64, id])
                .unwrap();
            for pair in std::iter::once(("__name__".to_string(), name)).chain(pairs) {
                let label = match interned.get(&pair) {
                    Some(&label) => label,
                    None => {
                        let label = match find
                            .query_row(params![pair.0, pair.1], |r| r.get(0))
                            .optional()
                            .unwrap()
                        {
                            Some(label) => label,
                            None => {
                                add.execute(params![pair.0, pair.1]).unwrap();
                                conn.last_insert_rowid()
                            }
                        };
                        interned.insert(pair, label);
                        label
                    }
                };
                post.execute(params![label, id]).unwrap();
            }
        }
    }
    // Posting-list lengths, so a selector can start from the smallest list.
    // The engine would maintain this count as series are created.
    conn.execute_batch(
        "UPDATE metrics_labels SET series =
           (SELECT count(*) FROM metrics_postings WHERE label_id = metrics_labels.id);
         COMMIT;",
    )
    .unwrap();
    let ms = started.elapsed().as_secs_f64() * 1_000.0;
    (ms, page_bytes(conn) - before)
}

fn page_bytes(conn: &Connection) -> u64 {
    let pages: i64 = conn
        .query_row("PRAGMA page_count", [], |r| r.get(0))
        .unwrap();
    let size: i64 = conn
        .query_row("PRAGMA page_size", [], |r| r.get(0))
        .unwrap();
    (pages * size) as u64
}

fn table_bytes(conn: &Connection, table: &str) -> u64 {
    conn.query_row(
        "SELECT coalesce(sum(pgsize), 0) FROM dbstat WHERE name = ?1",
        [table],
        |r| r.get::<_, i64>(0),
    )
    .map(|bytes| bytes as u64)
    .unwrap_or(0)
}

#[derive(Clone, Copy)]
enum Miss {
    /// `UNIQUE(name, canonical_labels)` on the durable catalog: no new table.
    Unique,
    /// `metrics_series_hash` keyed by the 64-bit identity hash.
    Hash,
}

/// One ingest cycle: resolve every series once, in write order.
fn resolve_cycle(conn: &Connection, series: usize, cache: &mut Cache, miss: Miss) -> (f64, usize) {
    let mut by_unique = conn
        .prepare_cached("SELECT id FROM metrics_series WHERE name = ?1 AND canonical_labels = ?2")
        .unwrap();
    let mut by_hash = conn
        .prepare_cached("SELECT series_id FROM metrics_series_hash WHERE hash = ?1")
        .unwrap();
    let mut hits = 0;
    let started = Instant::now();
    for index in 0..series {
        let (metric, pairs) = series_pairs(index);
        let hash = identity(metric, &pairs);
        if cache.get(hash).is_some() {
            hits += 1;
            continue;
        }
        let id: i64 = match miss {
            Miss::Unique => by_unique
                .query_row(params![metric, encode_labels(&pairs)], |r| r.get(0))
                .unwrap(),
            Miss::Hash => by_hash.query_row([hash as i64], |r| r.get(0)).unwrap(),
        };
        cache.put(hash, id);
    }
    (started.elapsed().as_secs_f64() * 1_000.0, hits)
}

/// The same per-series work with no lookup at all: what a cycle costs before
/// resolution, so the lookup's own share can be reported.
fn floor_cycle(series: usize) -> f64 {
    let started = Instant::now();
    let mut sink = 0u64;
    for index in 0..series {
        let (metric, pairs) = series_pairs(index);
        sink ^= identity(metric, &pairs);
    }
    std::hint::black_box(sink);
    started.elapsed().as_secs_f64() * 1_000.0
}

fn label_id(conn: &Connection, key: &str, value: &str) -> Option<i64> {
    conn.prepare_cached("SELECT id FROM metrics_labels WHERE key = ?1 AND value = ?2")
        .unwrap()
        .query_row([key, value], |r| r.get(0))
        .optional()
        .unwrap()
}

/// Series rows matching every label in `required` and, if given, any label
/// in `any_of`, with their labels decoded as a reader returns them. As
/// `find_series` does in memory, only the shortest required posting list is
/// scanned; every other condition is a primary-key probe per candidate.
fn select(conn: &Connection, required: &[i64], any_of: Option<&[i64]>) -> usize {
    let mut required: Vec<(i64, i64)> = required
        .iter()
        .map(|&id| {
            let series: i64 = conn
                .prepare_cached("SELECT series FROM metrics_labels WHERE id = ?1")
                .unwrap()
                .query_row([id], |r| r.get(0))
                .unwrap();
            (series, id)
        })
        .collect();
    required.sort_unstable();
    let (_, driver) = required[0];
    let mut sql = String::from(
        "SELECT s.id, s.canonical_labels FROM metrics_postings a \
         JOIN metrics_series s ON s.id = a.series_id WHERE a.label_id = ?",
    );
    let mut binds = vec![driver];
    for &(_, id) in &required[1..] {
        sql.push_str(
            " AND EXISTS (SELECT 1 FROM metrics_postings b \
             WHERE b.label_id = ? AND b.series_id = a.series_id)",
        );
        binds.push(id);
    }
    if let Some(any_of) = any_of {
        sql.push_str(
            " AND EXISTS (SELECT 1 FROM metrics_postings c WHERE c.series_id = a.series_id \
             AND c.label_id IN (",
        );
        sql.push_str(&vec!["?"; any_of.len()].join(","));
        sql.push_str("))");
        binds.extend_from_slice(any_of);
    }
    let mut stmt = conn.prepare_cached(&sql).unwrap();
    let mut rows = stmt.query(rusqlite::params_from_iter(binds)).unwrap();
    let mut n = 0;
    while let Some(row) = rows.next().unwrap() {
        let blob: Vec<u8> = row.get(1).unwrap();
        std::hint::black_box(decode_labels(&blob));
        n += 1;
    }
    n
}

fn timed(runs: usize, mut read: impl FnMut() -> usize) -> (f64, f64, f64, usize) {
    let first = Instant::now();
    let expected = read();
    let first_us = first.elapsed().as_secs_f64() * 1_000_000.0;
    for _ in 0..3 {
        assert_eq!(read(), expected);
    }
    let mut samples: Vec<f64> = (0..runs)
        .map(|_| {
            let started = Instant::now();
            assert_eq!(read(), expected);
            started.elapsed().as_secs_f64() * 1_000_000.0
        })
        .collect();
    samples.sort_by(f64::total_cmp);
    (
        first_us,
        samples[samples.len() / 2],
        samples[(samples.len() * 95).div_ceil(100) - 1],
        expected,
    )
}

pub(super) fn run(path: &str, series: usize, runs: usize) {
    let rss_start = status_kib("VmRSS:");
    let conn = Connection::open(path).unwrap();
    let (backfill_ms, added) = backfill(&conn);
    println!("disk,backfill_ms,{backfill_ms:.0},,");
    println!("disk,added_bytes,{added},,");
    let pairs_total: i64 = conn
        .query_row("SELECT count(*) FROM metrics_postings", [], |r| r.get(0))
        .unwrap();
    for table in [
        "metrics_labels",
        "sqlite_autoindex_metrics_labels_1",
        "metrics_postings",
        "metrics_series_hash",
    ] {
        println!("disk,bytes_{table},{},,", table_bytes(&conn, table));
    }
    println!(
        "disk,postings_bytes_per_row,{:.1},,{pairs_total}",
        table_bytes(&conn, "metrics_postings") as f64 / pairs_total as f64
    );
    println!(
        "disk,added_bytes_per_series,{:.0},,",
        added as f64 / series as f64
    );
    drop(conn);

    // Steady state: a fresh connection with SQLite's default page cache.
    let conn = Connection::open(path).unwrap();
    let floor = floor_cycle(series);
    println!("resolve,floor_cycle_ms,{floor:.0},,{series}");
    for budget in [100usize, 50, 10, 1] {
        let capacity = series * budget / 100;
        for (policy, miss) in [
            ("clock", Miss::Unique),
            ("clock", Miss::Hash),
            ("pinned", Miss::Hash),
        ] {
            let mut cache = match policy {
                "clock" => Cache::Clock {
                    slots: Vec::with_capacity(capacity),
                    map: HashMap::with_capacity(capacity),
                    hand: 0,
                    capacity,
                },
                _ => Cache::Pinned {
                    map: HashMap::with_capacity(capacity),
                    capacity,
                },
            };
            let miss_name = match miss {
                Miss::Unique => "unique",
                Miss::Hash => "hash",
            };
            // The first cycle fills the cache; the next two are the steady state.
            resolve_cycle(&conn, series, &mut cache, miss);
            let (ms, hits) = (0..2)
                .map(|_| resolve_cycle(&conn, series, &mut cache, miss))
                .min_by(|a, b| a.0.total_cmp(&b.0))
                .unwrap();
            println!(
                "resolve,{policy}_{miss_name}_{budget}pct_cycle_ms,{ms:.0},{:.0},{series}",
                (ms - floor).max(0.0) * 1_000_000.0 / series as f64
            );
            println!(
                "resolve,{policy}_{miss_name}_{budget}pct_hit_rate,{:.3},,",
                hits as f64 / series as f64
            );
            if let (100, Cache::Pinned { map, .. }) = (budget, &cache) {
                // hashbrown: one 16-byte (u64, i64) slot plus one control byte
                // per bucket.
                println!(
                    "memory,resolve_cache_full_bytes,{},,{}",
                    map.capacity() * 17,
                    map.len()
                );
            }
        }
    }

    // Reads: a fresh connection again, so the first run is page-cache cold.
    drop(conn);
    let conn = Connection::open(path).unwrap();
    let gateways = series / SERIES_PER_GATEWAY_PUB;
    let one_mac = mac_pub(gateways / 2);
    let metric = METRICS_PUB[1];
    let role_ids = |conn: &Connection| -> Vec<i64> {
        let mut stmt = conn
            .prepare_cached("SELECT id, value FROM metrics_labels WHERE key = 'role'")
            .unwrap();
        let regex = ["wan", "ssid"];
        stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))
            .unwrap()
            .map(Result::unwrap)
            .filter(|(_, value)| regex.contains(&value.as_str()))
            .map(|(id, _)| id)
            .collect()
    };
    let cases: Vec<(&str, Box<dyn Fn() -> usize + '_>)> = vec![
        (
            "select_name_and_mac",
            Box::new(|| {
                let name = label_id(&conn, "__name__", metric).unwrap();
                let mac = label_id(&conn, "cm_mac", &one_mac).unwrap();
                select(&conn, &[name, mac], None)
            }),
        ),
        (
            "select_exact_name",
            Box::new(|| {
                let name = label_id(&conn, "__name__", metric).unwrap();
                select(&conn, &[name], None)
            }),
        ),
        (
            "select_name_and_regex_role",
            Box::new(|| {
                let name = label_id(&conn, "__name__", metric).unwrap();
                select(&conn, &[name], Some(&role_ids(&conn)))
            }),
        ),
        (
            "discover_label_names",
            Box::new(|| {
                let mut stmt = conn
                    .prepare_cached("SELECT DISTINCT key FROM metrics_labels")
                    .unwrap();
                stmt.query_map([], |r| r.get::<_, String>(0))
                    .unwrap()
                    .count()
            }),
        ),
        (
            "discover_metric_names",
            Box::new(|| {
                let mut stmt = conn
                    .prepare_cached("SELECT value FROM metrics_labels WHERE key = '__name__'")
                    .unwrap();
                stmt.query_map([], |r| r.get::<_, String>(0))
                    .unwrap()
                    .count()
            }),
        ),
        (
            "discover_mac_values",
            Box::new(|| {
                let mut stmt = conn
                    .prepare_cached("SELECT value FROM metrics_labels WHERE key = 'cm_mac'")
                    .unwrap();
                stmt.query_map([], |r| r.get::<_, String>(0))
                    .unwrap()
                    .count()
            }),
        ),
    ];
    for (name, read) in cases {
        let (first, median, p95, rows) = timed(runs, read);
        println!("read,{name},{median:.0},{p95:.0},{rows}");
        println!("read,{name}_first_fresh_connection,{first:.0},,{rows}");
    }
    println!(
        "memory,process_rss_kib,{},,",
        status_kib("VmRSS:").saturating_sub(rss_start)
    );
    println!("memory,process_hwm_kib,{},,", status_kib("VmHWM:"));
    drop(conn);
    chunks(path, series, runs);
}

/// Series ids of `metric` with `cm_mac` (when given), from the disk postings.
fn selected_ids(conn: &Connection, metric: &str, mac: Option<&str>) -> Vec<i64> {
    let name = label_id(conn, "__name__", metric).unwrap();
    let mut sql = String::from("SELECT series_id FROM metrics_postings a WHERE a.label_id = ?1");
    if mac.is_some() {
        sql.push_str(
            " AND EXISTS (SELECT 1 FROM metrics_postings b WHERE b.label_id = ?2 \
             AND b.series_id = a.series_id)",
        );
    }
    let mut binds = vec![name];
    if let Some(mac) = mac {
        binds.push(label_id(conn, "cm_mac", mac).unwrap());
    }
    let mut stmt = conn.prepare(&sql).unwrap();
    stmt.query_map(rusqlite::params_from_iter(binds), |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// The chunk index on disk (phase 3): what the engine's in-memory
/// `BTreeMap<ChunkKey, ChunkMeta>` answers today, asked of `metrics_chunks`.
fn chunks(path: &str, series: usize, runs: usize) {
    let conn = Connection::open(path).unwrap();
    let chunk_rows: i64 = conn
        .query_row("SELECT count(*) FROM metrics_chunks", [], |r| r.get(0))
        .unwrap();
    println!("chunks,chunk_rows,{chunk_rows},,");
    println!(
        "chunks,bytes_metrics_chunks_series_ts,{},,",
        table_bytes(&conn, "metrics_chunks_series_ts")
    );
    let gateways = series / SERIES_PER_GATEWAY_PUB;
    let one_mac = mac_pub(gateways / 2);
    let metric = METRICS_PUB[1];
    let narrow = selected_ids(&conn, metric, Some(&one_mac));
    let broad = selected_ids(&conn, metric, None);
    const META: &str = "SELECT id, ts_min, ts_max, point_count, encoding FROM metrics_chunks \
                        WHERE series_id = ?1 AND resolution = 0 AND ts_min <= ?2 ORDER BY ts_min";
    const META_COVERED: &str = "SELECT id, ts_min, ts_max, point_count, encoding \
                        FROM metrics_chunks INDEXED BY metrics_chunks_meta \
                        WHERE series_id = ?1 AND resolution = 0 AND ts_min <= ?2 ORDER BY ts_min";
    const STATS: &str = "SELECT min(ts_min), max(ts_max), sum(point_count), count(*) \
                         FROM metrics_chunks WHERE series_id = ?1 AND resolution = 0";
    const BLOBS: &str = "SELECT ts_data, val_data FROM metrics_chunks WHERE id = ?1";
    let stop = i64::MAX;
    let lookup = |sql: &str, ids: &[i64], fetch: bool| -> usize {
        let mut meta = conn.prepare_cached(sql).unwrap();
        let mut blobs = conn.prepare_cached(BLOBS).unwrap();
        let mut found = 0;
        for &id in ids {
            let rows: Vec<i64> = meta
                .query_map(params![id, stop], |r| r.get(0))
                .unwrap()
                .map(Result::unwrap)
                .collect();
            found += rows.len();
            if fetch {
                for chunk in rows {
                    let (ts, val): (Vec<u8>, Vec<u8>) = blobs
                        .query_row([chunk], |r| Ok((r.get(0)?, r.get(1)?)))
                        .unwrap();
                    std::hint::black_box((ts, val));
                }
            }
        }
        found
    };
    let stats = |ids: &[i64]| -> usize {
        let mut stmt = conn.prepare_cached(STATS).unwrap();
        ids.iter()
            .map(|&id| {
                stmt.query_row([id], |r| r.get::<_, i64>(3)).unwrap() as usize
            })
            .sum()
    };
    let emit = |name: &str, rows: usize, read: &dyn Fn() -> usize| {
        let (first, median, p95, got) = timed(runs, read);
        assert_eq!(got, rows, "{name}");
        println!("chunks,{name},{median:.0},{p95:.0},{rows}");
        println!("chunks,{name}_first_fresh_connection,{first:.0},,{rows}");
    };
    let narrow_chunks = lookup(META, &narrow, false);
    let broad_chunks = lookup(META, &broad, false);
    emit("narrow_meta", narrow_chunks, &|| lookup(META, &narrow, false));
    emit("narrow_meta_and_blobs", narrow_chunks, &|| lookup(META, &narrow, true));
    emit("narrow_stats", narrow_chunks, &|| stats(&narrow));
    emit("broad_meta", broad_chunks, &|| lookup(META, &broad, false));
    emit("broad_meta_and_blobs", broad_chunks, &|| lookup(META, &broad, true));
    emit("broad_stats", broad_chunks, &|| stats(&broad));

    // A covering index: metadata reads never touch the payload rows.
    let before = page_bytes(&conn);
    let started = Instant::now();
    conn.execute_batch(
        "CREATE INDEX metrics_chunks_meta ON metrics_chunks
           (series_id, resolution, ts_min, ts_max, point_count, encoding)",
    )
    .unwrap();
    println!(
        "chunks,covering_index_build_ms,{:.0},,",
        started.elapsed().as_secs_f64() * 1_000.0
    );
    println!("chunks,covering_index_bytes,{},,", page_bytes(&conn) - before);
    emit("narrow_meta_covered", narrow_chunks, &|| {
        lookup(META_COVERED, &narrow, false)
    });
    emit("broad_meta_covered", broad_chunks, &|| {
        lookup(META_COVERED, &broad, false)
    });

    // One statement for a whole selection: postings joined to the covering
    // index, then to the payload rows, in (series, ts) order, the batched
    // shape of today's ordered chunk reader.
    let name_label = label_id(&conn, "__name__", metric).unwrap();
    let joined = |blobs: bool| -> usize {
        let sql = if blobs {
            "SELECT c.ts_data, c.val_data FROM metrics_postings p \
             JOIN metrics_chunks c INDEXED BY metrics_chunks_meta \
               ON c.series_id = p.series_id AND c.resolution = 0 \
             WHERE p.label_id = ?1 ORDER BY p.series_id, c.ts_min"
        } else {
            "SELECT c.id, c.ts_min, c.ts_max, c.point_count FROM metrics_postings p \
             JOIN metrics_chunks c INDEXED BY metrics_chunks_meta \
               ON c.series_id = p.series_id AND c.resolution = 0 \
             WHERE p.label_id = ?1 ORDER BY p.series_id, c.ts_min"
        };
        let mut stmt = conn.prepare_cached(sql).unwrap();
        let mut rows = stmt.query([name_label]).unwrap();
        let mut n = 0;
        while let Some(row) = rows.next().unwrap() {
            if blobs {
                let ts: Vec<u8> = row.get(0).unwrap();
                let val: Vec<u8> = row.get(1).unwrap();
                std::hint::black_box((ts, val));
            }
            n += 1;
        }
        n
    };
    emit("broad_meta_joined", broad_chunks, &|| joined(false));
    emit("broad_meta_and_blobs_joined", broad_chunks, &|| joined(true));

    // Compaction planning: every series with two or more raw chunks, from a
    // scan of the covering index.
    let plan = || -> usize {
        conn.prepare_cached(
            "SELECT series_id, count(*) FROM metrics_chunks INDEXED BY metrics_chunks_meta \
             WHERE resolution = 0 AND encoding = 1 GROUP BY series_id HAVING count(*) >= 2",
        )
        .unwrap()
        .query_map([], |r| r.get::<_, i64>(0))
        .unwrap()
        .count()
    };
    let (first, median, p95, groups) = timed(runs.min(3), plan);
    println!("chunks,compaction_plan_full_scan,{median:.0},{p95:.0},{groups}");
    println!("chunks,compaction_plan_full_scan_first,{first:.0},,{groups}");
    println!(
        "chunks,process_hwm_kib,{},,",
        status_kib("VmHWM:")
    );
}
