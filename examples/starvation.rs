//! Isolates the async-starvation effect that the live pipeline is too
//! upstream-bound to show.
//!
//! In production the RPC delivers roughly one block per second, so the CPU
//! stage occupies the runtime only a few percent of the time and moving it off
//! the runtime changes almost nothing. This benchmark removes the network and
//! runs the *real* `analyse` function back to back on a realistically sized
//! block, first directly on the async runtime and then via `spawn_blocking`.
//!
//! Responsiveness is measured the way an HTTP handler experiences it: a task
//! sleeps for 1 ms in a loop and records how far past 1 ms it actually woke.
//! Oversleep is time the runtime could not get back to the task.
//!
//!   cargo run --release --example starvation

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use solscope::ingest::analyse;
use solscope::model::RpcBlock;

const PROBE_INTERVAL: Duration = Duration::from_millis(1);

fn realistic_block() -> RpcBlock {
    let mut block: RpcBlock = serde_json::from_str(include_str!("../tests/fixtures/block.json"))
        .expect("fixture should parse");

    // The fixture is a trimmed sample; repeat it up to the size of a real
    // mainnet block so the analysis cost per block is representative.
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

fn run(label: &str, on_runtime: bool, block: Arc<RpcBlock>, rounds: usize) {
    // One worker thread makes the contention unambiguous: any CPU work left on
    // the runtime is time the probe cannot run.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("runtime");

    let oversleep = rt.block_on(async move {
        let stop = Arc::new(AtomicBool::new(false));

        let probe = {
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut samples = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let t0 = Instant::now();
                    tokio::time::sleep(PROBE_INTERVAL).await;
                    let late = t0.elapsed().saturating_sub(PROBE_INTERVAL);
                    samples.push(late.as_secs_f64() * 1000.0);
                }
                samples
            })
        };

        let started = Instant::now();

        // The analysis must be a spawned task, not driven by block_on: block_on
        // runs on the calling thread, so leaving it there would never contend
        // with the probe for the worker. The real pipeline spawns its analysers.
        let load = tokio::spawn(async move {
            for _ in 0..rounds {
                if on_runtime {
                    // Exactly what a CPU-heavy stage written the obvious way does.
                    let _ = analyse(1, &block, false);
                    tokio::task::yield_now().await;
                } else {
                    let b = block.clone();
                    let _ = tokio::task::spawn_blocking(move || analyse(1, &b, false)).await;
                }
            }
        });
        load.await.expect("load task");
        let elapsed = started.elapsed();

        stop.store(true, Ordering::Relaxed);
        let mut samples = probe.await.expect("probe");
        samples.sort_by(|a, b| a.partial_cmp(b).unwrap());

        println!("{label}");
        println!(
            "  analysed        {rounds} blocks in {:.2}s",
            elapsed.as_secs_f64()
        );
        println!("  probe samples   {}", samples.len());
        println!("  oversleep p50   {:8.3} ms", percentile(&samples, 50.0));
        println!("  oversleep p95   {:8.3} ms", percentile(&samples, 95.0));
        println!("  oversleep p99   {:8.3} ms", percentile(&samples, 99.0));
        println!(
            "  oversleep max   {:8.3} ms",
            samples.last().copied().unwrap_or(f64::NAN)
        );
        println!();

        percentile(&samples, 99.0)
    });

    let _ = oversleep;
}

fn main() {
    let block = Arc::new(realistic_block());
    println!(
        "block under analysis: {} transactions\nprobe: sleep({} ms) in a loop, measuring oversleep\n",
        block.transactions.len(),
        PROBE_INTERVAL.as_millis()
    );

    // Warm up so the first run does not pay for lazy allocation.
    let _ = analyse(1, &block, false);

    run("CPU stage ON the async runtime", true, block.clone(), 40);
    run("CPU stage via spawn_blocking", false, block, 40);
}
