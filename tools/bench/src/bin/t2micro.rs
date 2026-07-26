//! t2micro: per-statement timing of the Tier 2 blob insert path.
//! Same workload shape as bench (10 blobs x 100k points, 1000 series),
//! but times each INSERT statement separately to attribute first-touch
//! (series-catalog) cost vs steady-state cost.
//!
//!   t2micro <path-to-extension> [iterations]

use std::env;
use std::fs;
use std::time::Instant;

use rusqlite::{params, Connection};

const N_SERIES: usize = 1000;
const BLOBS: usize = 10;
const STEPS_PER_BLOB: usize = 100; // 100 steps x 1000 series = 100k pts/blob

fn encode_blob(blob_idx: usize) -> Vec<u8> {
    let n_points = STEPS_PER_BLOB * N_SERIES;
    let mut out = Vec::with_capacity(64 * 1024 + n_points * 20);
    out.push(0x01);
    out.push(0x00);
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&(N_SERIES as u32).to_le_bytes());
    out.extend_from_slice(&(n_points as u32).to_le_bytes());
    for s in 0..N_SERIES {
        let name = format!("metric.{:03}", s % 10);
        let labels = format!("{{\"host\":\"host-{:03}\"}}", s / 10);
        out.extend_from_slice(&(name.len() as u32).to_le_bytes());
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&(labels.len() as u32).to_le_bytes());
        out.extend_from_slice(labels.as_bytes());
    }
    for _ in 0..STEPS_PER_BLOB {
        for s in 0..N_SERIES {
            out.extend_from_slice(&(s as u32).to_le_bytes());
        }
    }
    let base = 1_700_000_000_000i64 + (blob_idx * STEPS_PER_BLOB) as i64 * 10_000;
    for i in 0..STEPS_PER_BLOB {
        for _ in 0..N_SERIES {
            out.extend_from_slice(&(base + i as i64 * 10_000).to_le_bytes());
        }
    }
    for i in 0..STEPS_PER_BLOB {
        for s in 0..N_SERIES {
            out.extend_from_slice(&((s * 7 + i) as f64 * 0.31).to_le_bytes());
        }
    }
    out
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let ext = args.get(1).expect("usage: t2micro <ext> [iters]");
    let iters: usize = args.get(2).map(|s| s.parse().unwrap()).unwrap_or(5);

    let blobs: Vec<Vec<u8>> = (0..BLOBS).map(encode_blob).collect();

    // per-statement times in ms, [iteration][blob]
    let mut times = vec![vec![0f64; BLOBS]; iters];

    for it in 0..iters {
        let path = format!("/tmp/tl_t2micro_{it}.db");
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(format!("{path}-wal"));
        let _ = fs::remove_file(format!("{path}-journal"));
        let conn = Connection::open(&path).expect("open db");
        unsafe {
            conn.load_extension_enable().unwrap();
            conn.load_extension(ext, None::<&str>).unwrap();
        }
        conn.load_extension_disable().unwrap();
        conn.execute_batch("CREATE VIRTUAL TABLE metrics USING timeless_metrics;")
            .unwrap();

        conn.execute_batch("BEGIN").unwrap();
        {
            let mut stmt = conn
                .prepare("INSERT INTO metrics(metrics) VALUES (?1)")
                .unwrap();
            for (b, blob) in blobs.iter().enumerate() {
                let t = Instant::now();
                stmt.execute(params![blob]).unwrap();
                times[it][b] = t.elapsed().as_secs_f64() * 1e3;
            }
        }
        conn.execute_batch("COMMIT").unwrap();
        drop(conn);
        let _ = fs::remove_file(&path);
    }

    // median per blob position across iterations
    print!("blob-ms:");
    for b in 0..BLOBS {
        let mut col: Vec<f64> = (0..iters).map(|i| times[i][b]).collect();
        col.sort_by(|a, b| a.partial_cmp(b).unwrap());
        print!(" {:.2}", col[iters / 2]);
    }
    println!();
    let totals: Vec<f64> = (0..iters).map(|i| times[i].iter().sum()).collect();
    let mut sorted = totals.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    println!(
        "total-ms median {:.1}  (first-blob median {:.2}, steady median {:.2})",
        sorted[iters / 2],
        {
            let mut c: Vec<f64> = (0..iters).map(|i| times[i][0]).collect();
            c.sort_by(|a, b| a.partial_cmp(b).unwrap());
            c[iters / 2]
        },
        {
            let mut c: Vec<f64> = (0..iters).flat_map(|i| times[i][1..].to_vec()).collect();
            c.sort_by(|a, b| a.partial_cmp(b).unwrap());
            c[c.len() / 2]
        }
    );
}
