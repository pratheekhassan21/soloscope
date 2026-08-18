use std::path::PathBuf;

use clap::Parser;

/// Solana block ingestion, contention analysis and OHLCV indexing in one binary.
#[derive(Parser, Debug, Clone)]
#[command(name = "solscope", version, about)]
pub struct Config {
    /// Solana JSON-RPC endpoint. Reads SOLSCOPE_RPC_URL (or a .env file) when omitted.
    #[arg(long, env = "SOLSCOPE_RPC_URL", hide_env_values = true)]
    pub rpc_url: Option<String>,

    /// First slot to ingest. Defaults to (latest finalized slot - slot-lag - slots).
    #[arg(long)]
    pub start_slot: Option<u64>,

    /// Number of consecutive slots to ingest.
    #[arg(long, default_value_t = 1000)]
    pub slots: u64,

    /// How far behind the tip to start when --start-slot is not given.
    #[arg(long, default_value_t = 2000)]
    pub slot_lag: u64,

    /// SQLite database path.
    #[arg(long, default_value = "solscope.db")]
    pub db: PathBuf,

    /// HTTP listen port for the API and dashboard.
    #[arg(long, default_value_t = 8080)]
    pub port: u16,

    /// Self-imposed upstream request cap, in requests per second.
    #[arg(long, default_value_t = 10.0)]
    pub rps: f64,

    /// Concurrent block fetch workers.
    #[arg(long, default_value_t = 8)]
    pub fetch_workers: usize,

    /// Capacity of the fetch -> analyse channel. Kept small because each slot
    /// in flight holds a fully deserialized block (megabytes on a busy slot).
    #[arg(long, default_value_t = 8)]
    pub fetch_channel_capacity: usize,

    /// Capacity of the analyse -> writer channel.
    #[arg(long, default_value_t = 64)]
    pub channel_capacity: usize,

    /// Concurrent block analysis workers.
    #[arg(long, default_value_t = 4)]
    pub analyze_workers: usize,

    /// Blocks committed per SQLite write transaction.
    #[arg(long, default_value_t = 32)]
    pub batch_blocks: usize,

    /// Serve the API only; skip ingestion.
    #[arg(long, default_value_t = false)]
    pub serve_only: bool,

    /// Async runtime worker threads. Defaults to the core count; the
    /// async-starvation experiment lowers it to make contention for runtime
    /// threads visible.
    #[arg(long)]
    pub runtime_workers: Option<usize>,

    /// Persist the resolved lock sets for vote transactions too. They are ~60%
    /// of all transactions and their locks are trivial, so they are dropped by
    /// default; contention metrics always include them either way.
    #[arg(long, default_value_t = false)]
    pub store_vote_locks: bool,

    /// Run block analysis directly on the async runtime instead of a blocking pool.
    /// Used by the async-starvation experiment; the default is the correct setting.
    #[arg(long, default_value_t = false)]
    pub analyze_on_runtime: bool,

    /// Enable the /api/debug/* endpoints used by the load experiments.
    #[arg(long, default_value_t = false)]
    pub debug_endpoints: bool,

    /// Exit once ingestion finishes instead of continuing to serve.
    #[arg(long, default_value_t = false)]
    pub exit_after_ingest: bool,
}

impl Config {
    /// Inclusive-exclusive slot range, once the start slot is known.
    pub fn range_from(&self, start: u64) -> std::ops::Range<u64> {
        start..start + self.slots
    }
}
