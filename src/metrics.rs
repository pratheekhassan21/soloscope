

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering::Relaxed};
use std::time::Instant;

use crate::ohlcv::Exclusions;

#[derive(Debug)]
pub struct Metrics {
    pub start: Instant,
    pub slots_total: AtomicU64,
    pub slots_done: AtomicU64,
    pub slots_skipped: AtomicU64,
    pub slots_failed: AtomicU64,
    pub slots_already_present: AtomicU64,
    pub transactions: AtomicU64,
    pub trades: AtomicU64,
    pub blocks_written: AtomicU64,
    pub write_batches: AtomicU64,
    /// Time spent inside SQLite commits. Isolates write-path cost from the
    /// upstream latency that otherwise dominates wall-clock throughput.
    pub commit_nanos: AtomicU64,
    pub rows_written: AtomicU64,
    /// Blocks fetched but not yet analysed.
    pub fetch_queue: AtomicI64,
    /// Analysed blocks waiting on the database writer.
    pub write_queue: AtomicI64,
    pub fetch_queue_max: AtomicI64,
    pub write_queue_max: AtomicI64,
    pub writer_paused: AtomicBool,
    pub ingest_done: AtomicBool,
    pub exclusions: Mutex<Exclusions>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            start: Instant::now(),
            slots_total: AtomicU64::new(0),
            slots_done: AtomicU64::new(0),
            slots_skipped: AtomicU64::new(0),
            slots_failed: AtomicU64::new(0),
            slots_already_present: AtomicU64::new(0),
            transactions: AtomicU64::new(0),
            trades: AtomicU64::new(0),
            blocks_written: AtomicU64::new(0),
            write_batches: AtomicU64::new(0),
            commit_nanos: AtomicU64::new(0),
            rows_written: AtomicU64::new(0),
            fetch_queue: AtomicI64::new(0),
            write_queue: AtomicI64::new(0),
            fetch_queue_max: AtomicI64::new(0),
            write_queue_max: AtomicI64::new(0),
            writer_paused: AtomicBool::new(false),
            ingest_done: AtomicBool::new(false),
            exclusions: Mutex::new(Exclusions::default()),
        }
    }
}

impl Metrics {
    pub fn enter_fetch_queue(&self) {
        let depth = self.fetch_queue.fetch_add(1, Relaxed) + 1;
        self.fetch_queue_max.fetch_max(depth, Relaxed);
    }

    pub fn leave_fetch_queue(&self) {
        self.fetch_queue.fetch_sub(1, Relaxed);
    }

    pub fn enter_write_queue(&self) {
        let depth = self.write_queue.fetch_add(1, Relaxed) + 1;
        self.write_queue_max.fetch_max(depth, Relaxed);
    }

    pub fn leave_write_queue(&self) {
        self.write_queue.fetch_sub(1, Relaxed);
    }

    pub fn record_exclusions(&self, other: &Exclusions) {
        if let Ok(mut e) = self.exclusions.lock() {
            e.merge(other);
        }
    }

    /// Slots resolved one way or another, including ones a previous run finished.
    pub fn slots_settled(&self) -> u64 {
        self.slots_done.load(Relaxed)
            + self.slots_skipped.load(Relaxed)
            + self.slots_failed.load(Relaxed)
            + self.slots_already_present.load(Relaxed)
    }

    /// Total seconds spent committing to SQLite.
    pub fn commit_secs(&self) -> f64 {
        self.commit_nanos.load(Relaxed) as f64 / 1e9
    }

    pub fn elapsed_secs(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }
}
