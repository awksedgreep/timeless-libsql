//! t2micro: per-statement timing of the Tier 2 blob insert path.
//! Same workload shape as bench (10 blobs x 100k points, 1000 series),
//! but times each INSERT statement separately to attribute first-touch
//! (series-catalog) cost vs steady-state cost.
//!
//!   t2micro <path-to-extension> [iterations] [n_blobs] [steps_per_blob]
//!
//! Default 10 blobs x 100 steps mirrors bench's Tier 2 shape (1M points
//! total). Pass e.g. `1 1000` to send the same million points as ONE
//! statement and isolate per-statement overhead.

use std::env;
use std::time::Instant;

use rusqlite::{params, Connection};

const N_SERIES: usize = 1000;

fn encode_blob(blob_idx: usize, steps: usize) -> Vec<u8> {
    let n_points = steps * N_SERIES;
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
    for _ in 0..steps {
        for s in 0..N_SERIES {
            out.extend_from_slice(&(s as u32).to_le_bytes());
        }
    }
    let base = 1_700_000_000_000i64 + (blob_idx * steps) as i64 * 10_000;
    for i in 0..steps {
        for _ in 0..N_SERIES {
            out.extend_from_slice(&(base + i as i64 * 10_000).to_le_bytes());
        }
    }
    for i in 0..steps {
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

    let n_blobs: usize = args.get(3).map(|s| s.parse().unwrap()).unwrap_or(10);
    let steps: usize = args.get(4).map(|s| s.parse().unwrap()).unwrap_or(100);
    let blobs: Vec<Vec<u8>> = (0..n_blobs).map(|b| encode_blob(b, steps)).collect();

    // per-statement times in ms, [iteration][blob]
    let mut times = vec![vec![0f64; n_blobs]; iters];

    let temporary = tempfile::Builder::new()
        .prefix("timeless-t2micro-")
        .tempdir()
        .expect("create t2micro scratch directory");
    for (it, iteration_times) in times.iter_mut().enumerate() {
        let path = temporary.path().join(format!("iteration-{it}.db"));
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
                iteration_times[b] = t.elapsed().as_secs_f64() * 1e3;
            }
        }
        conn.execute_batch("COMMIT").unwrap();
        drop(conn);
    }

    // median per blob position across iterations
    print!("blob-ms:");
    for mut col in (0..n_blobs).map(|blob| {
        times
            .iter()
            .map(|iteration| iteration[blob])
            .collect::<Vec<_>>()
    }) {
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
            if c.is_empty() {
                f64::NAN
            } else {
                c[c.len() / 2]
            }
        }
    );
}
