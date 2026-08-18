//! Write-path benchmark: batch size, transaction boundaries, and concurrent
//! reads against writes.
//!
//! The live pipeline cannot answer these questions. Upstream delivers roughly
//! one block per second while the writer can absorb tens per second, so the
//! write channel is empty whenever the writer wakes and every commit contains a
//! single block no matter what `--batch-blocks` says. This benchmark removes
//! the network and hands the writer a real backlog, which is the only condition
//! under which batch size means anything.
//!
//!   cargo run --release --example writepath

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rusqlite::Connection;
use solscope::db::{self, BlockWrite};
use solscope::ingest::analyse;
use solscope::model::RpcBlock;

const BLOCKS: usize = 200;

fn realistic_block() -> RpcBlock {
    let mut block: RpcBlock = serde_json::from_str(include_str!("../tests/fixtures/block.json"))
        .expect("fixture should parse");
    let base = block.transactions.clone();
    while block.transactions.len() < 1_100 {
        block.transactions.extend(base.iter().cloned());
    }
    block
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let k = (sorted.len() - 1) as f64 * (p / 100.0);
    let (lo, hi) = (
        k.floor() as usize,
        (k.ceil() as usize).min(sorted.len() - 1),
    );
    sorted[lo] + (sorted[hi] - sorted[lo]) * (k - k.floor())
}

/// Commits `writes` in batches of `batch`, returning (seconds, rows).
fn commit_all(conn: &mut Connection, writes: &[BlockWrite], batch: usize) -> (f64, usize) {
    let mut rows = 0;
    let started = Instant::now();

    for chunk in writes.chunks(batch) {
        let tx = conn.transaction().expect("begin");
        for b in chunk {
            db::write_block(&tx, b).expect("write");
            rows += b.transactions.len() + b.trades.len() + b.conflicts.len() + 1;
        }
        tx.commit().expect("commit");
    }

    (started.elapsed().as_secs_f64(), rows)
}

fn prepared_writes(block: &RpcBlock, n: usize) -> Vec<BlockWrite> {
    // Distinct slots so every block is an insert rather than a replace.
    (0..n)
        .map(|i| {
            let (mut w, _) = analyse(1_000_000 + i as u64, block, false);
            w.slot = 1_000_000 + i as u64;
            for t in &mut w.transactions {
                // Signatures are the primary key; keep them unique per slot.
                t.signature = format!("{}-{}", w.slot, t.tx_index);
            }
            for t in &mut w.trades {
                t.signature = format!("{}-{}", w.slot, t.tx_index);
            }
            w
        })
        .collect()
}

fn bench_batches(writes: &[BlockWrite]) {
    println!("== batch size (WAL, synchronous=NORMAL) ==");
    println!(
        "{:>7}  {:>9}  {:>10}  {:>12}  {:>9}",
        "batch", "commits", "commit s", "rows/s", "db MB"
    );

    for batch in [1usize, 8, 32, 128, 512] {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("bench.db");
        let mut conn = db::open_writer(&path).expect("open");

        let (secs, rows) = commit_all(&mut conn, writes, batch);
        let commits = writes.len().div_ceil(batch);
        let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        println!(
            "{batch:>7}  {commits:>9}  {secs:>10.2}  {:>12.0}  {:>9.0}",
            rows as f64 / secs,
            size as f64 / 1_048_576.0
        );
    }
    println!();
}

/// The same sweep with `synchronous=FULL`, where every commit forces an fsync.
/// This is what makes batch size matter, and its absence under NORMAL is why
/// the sweep above is flat.
fn bench_batches_durable(writes: &[BlockWrite]) {
    println!("== batch size (WAL, synchronous=FULL -- fsync per commit) ==");
    println!(
        "{:>7}  {:>9}  {:>10}  {:>12}",
        "batch", "commits", "commit s", "rows/s"
    );

    for batch in [1usize, 8, 32, 128, 512] {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("bench.db");
        let mut conn = db::open_writer(&path).expect("open");
        conn.pragma_update(None, "synchronous", "FULL")
            .expect("synchronous");

        let (secs, rows) = commit_all(&mut conn, writes, batch);
        let commits = writes.len().div_ceil(batch);
        println!(
            "{batch:>7}  {commits:>9}  {secs:>10.2}  {:>12.0}",
            rows as f64 / secs
        );
    }
    println!();
}

fn bench_journal_modes(writes: &[BlockWrite]) {
    println!("== journal mode (batch = 32) ==");
    println!("{:>10}  {:>10}  {:>12}", "mode", "commit s", "rows/s");

    for mode in ["WAL", "DELETE"] {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("bench.db");
        let mut conn = db::open_writer(&path).expect("open");
        conn.pragma_update(None, "journal_mode", mode)
            .expect("journal_mode");

        let (secs, rows) = commit_all(&mut conn, writes, 32);
        println!("{mode:>10}  {secs:>10.2}  {:>12.0}", rows as f64 / secs);
    }
    println!();
}

/// Reader latency while the writer is committing continuously. This is the
/// property that keeps the API responsive during ingestion.
fn bench_concurrent_reads(writes: &[BlockWrite]) {
    println!("== read latency during sustained writes (batch = 32) ==");
    println!(
        "{:>10}  {:>8}  {:>8}  {:>8}  {:>8}  {:>7}",
        "mode", "p50 ms", "p95 ms", "p99 ms", "max ms", "reads"
    );

    for mode in ["WAL", "DELETE"] {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("bench.db");
        let mut conn = db::open_writer(&path).expect("open");
        conn.pragma_update(None, "journal_mode", mode)
            .expect("journal_mode");

        // Seed some rows so the reader has something to scan.
        commit_all(&mut conn, &writes[..20.min(writes.len())], 32);

        let stop = Arc::new(AtomicBool::new(false));
        let reader = {
            let stop = stop.clone();
            let path = path.clone();
            std::thread::spawn(move || {
                let conn = db::open_reader(&path).expect("open reader");
                let mut samples = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let t0 = Instant::now();
                    let _: i64 = conn
                        .query_row(
                            "SELECT COUNT(*) FROM transactions WHERE slot = ?1",
                            [1_000_005i64],
                            |r| r.get(0),
                        )
                        .unwrap_or(0);
                    samples.push(t0.elapsed().as_secs_f64() * 1000.0);
                    std::thread::sleep(Duration::from_millis(2));
                }
                samples
            })
        };

        commit_all(&mut conn, writes, 32);
        stop.store(true, Ordering::Relaxed);

        let mut samples = reader.join().expect("reader");
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());

        println!(
            "{mode:>10}  {:>8.3}  {:>8.3}  {:>8.3}  {:>8.3}  {:>7}",
            percentile(&samples, 50.0),
            percentile(&samples, 95.0),
            percentile(&samples, 99.0),
            samples.last().copied().unwrap_or(f64::NAN),
            samples.len()
        );
    }
    println!();
}

fn main() {
    let block = realistic_block();
    println!(
        "preparing {BLOCKS} blocks of {} transactions each...\n",
        block.transactions.len()
    );
    let writes = prepared_writes(&block, BLOCKS);
    let rows: usize = writes
        .iter()
        .map(|b| b.transactions.len() + b.trades.len() + b.conflicts.len() + 1)
        .sum();
    println!("{rows} rows per pass\n");

    bench_batches(&writes);
    bench_batches_durable(&writes);
    bench_journal_modes(&writes);
    bench_concurrent_reads(&writes);
}
