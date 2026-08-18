
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags, params};

pub const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS slots (
  slot        INTEGER PRIMARY KEY,
  status      TEXT    NOT NULL,
  block_time  INTEGER,
  tx_count    INTEGER NOT NULL DEFAULT 0,
  updated_at  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS transactions (
  signature   TEXT    NOT NULL,
  slot        INTEGER NOT NULL,
  tx_index    INTEGER NOT NULL,
  succeeded   INTEGER NOT NULL,
  fee         INTEGER NOT NULL,
  is_vote     INTEGER NOT NULL,
  step        INTEGER NOT NULL,
  program_ids TEXT    NOT NULL,
  writable    TEXT    NOT NULL,
  readonly    TEXT    NOT NULL,
  PRIMARY KEY (slot, tx_index)
);
CREATE INDEX IF NOT EXISTS idx_tx_sig ON transactions(signature);

CREATE TABLE IF NOT EXISTS contention (
  slot            INTEGER PRIMARY KEY,
  block_time      INTEGER,
  tx_count        INTEGER NOT NULL,
  depth           INTEGER NOT NULL,
  widths          TEXT    NOT NULL,
  tx_count_novote INTEGER NOT NULL,
  depth_novote    INTEGER NOT NULL,
  widths_novote   TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS account_conflicts (
  slot        INTEGER NOT NULL,
  account     TEXT    NOT NULL,
  write_locks INTEGER NOT NULL,
  read_locks  INTEGER NOT NULL,
  delays      INTEGER NOT NULL,
  programs    TEXT    NOT NULL,
  PRIMARY KEY (slot, account)
);
CREATE INDEX IF NOT EXISTS idx_conflicts_account ON account_conflicts(account);

CREATE TABLE IF NOT EXISTS trades (
  signature    TEXT    NOT NULL,
  mint         TEXT    NOT NULL,
  slot         INTEGER NOT NULL,
  block_time   INTEGER NOT NULL,
  tx_index     INTEGER NOT NULL,
  token_amount REAL    NOT NULL,
  sol_amount   REAL    NOT NULL,
  price_sol    REAL    NOT NULL,
  PRIMARY KEY (signature, mint)
);
CREATE INDEX IF NOT EXISTS idx_trades_mint_time ON trades(mint, block_time);
-- Covering index for the /api/tokens aggregate, which would otherwise scan the
-- index and then fetch every row for sol_amount.
CREATE INDEX IF NOT EXISTS idx_trades_mint_agg ON trades(mint, sol_amount, block_time);
CREATE INDEX IF NOT EXISTS idx_trades_slot ON trades(slot);

CREATE TABLE IF NOT EXISTS candles (
  mint         TEXT    NOT NULL,
  interval_s   INTEGER NOT NULL,
  bucket_ts    INTEGER NOT NULL,
  open         REAL    NOT NULL,
  high         REAL    NOT NULL,
  low          REAL    NOT NULL,
  close        REAL    NOT NULL,
  volume_sol   REAL    NOT NULL,
  volume_token REAL    NOT NULL,
  trade_count  INTEGER NOT NULL,
  PRIMARY KEY (mint, interval_s, bucket_ts)
);
"#;


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotStatus {
    Done,
    Skipped,
    Failed,
}

impl SlotStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            SlotStatus::Done => "done",
            SlotStatus::Skipped => "skipped",
            SlotStatus::Failed => "failed",
        }
    }
}




#[derive(Debug, Clone)]
pub struct TxRow {
    pub signature: String,
    pub tx_index: usize,
    pub succeeded: bool,
    pub fee: u64,
    pub is_vote: bool,
    pub step: usize,
    pub program_ids: String,
    pub writable: String,
    pub readonly: String,
}

#[derive(Debug, Clone)]
pub struct ContentionRow {
    pub tx_count: usize,
    pub depth: usize,
    pub widths: String,
    pub tx_count_novote: usize,
    pub depth_novote: usize,
    pub widths_novote: String,
}

#[derive(Debug, Clone)]
pub struct AccountConflictRow {
    pub account: String,
    pub write_locks: u32,
    pub read_locks: u32,
    pub delays: u32,
    pub programs: String,
}

#[derive(Debug, Clone)]
pub struct TradeRow {
    pub signature: String,
    pub mint: String,
    pub tx_index: usize,
    pub token_amount: f64,
    pub sol_amount: f64,
    pub price_sol: f64,
}

/// Everything produced for one slot, committed atomically.
#[derive(Debug, Clone)]
pub struct BlockWrite {
    pub slot: u64,
    pub status: SlotStatus,
    pub block_time: Option<i64>,
    pub transactions: Vec<TxRow>,
    pub contention: Option<ContentionRow>,
    pub conflicts: Vec<AccountConflictRow>,
    pub trades: Vec<TradeRow>,
}

impl BlockWrite {
    pub fn skipped(slot: u64) -> Self {
        Self {
            slot,
            status: SlotStatus::Skipped,
            block_time: None,
            transactions: Vec::new(),
            contention: None,
            conflicts: Vec::new(),
            trades: Vec::new(),
        }
    }

    pub fn failed(slot: u64) -> Self {
        Self {
            status: SlotStatus::Failed,
            ..Self::skipped(slot)
        }
    }
}


pub enum WriteMsg {
    Block(Box<BlockWrite>),
    /// Stall the writer, used by the backpressure experiment to fill the channel.
    Pause(Duration),
    /// Rebuild candles from the trades table and reply when finished.
    RebuildCandles(tokio::sync::oneshot::Sender<Result<usize, String>>),
    Shutdown(tokio::sync::oneshot::Sender<()>),
}



pub fn open_writer(path: &Path) -> Result<Connection> {
    let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
   
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.busy_timeout(Duration::from_secs(10))?;
    conn.execute_batch(SCHEMA)?;
    Ok(conn)
}

pub fn open_reader(path: &Path) -> Result<Connection> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )
    .with_context(|| format!("opening {} read-only", path.display()))?;
    conn.busy_timeout(Duration::from_secs(10))?;
    conn.pragma_update(None, "query_only", true)?;
    Ok(conn)
}


pub fn completed_slots(conn: &Connection, from: u64, to: u64) -> Result<Vec<u64>> {
    let mut stmt = conn.prepare(
        "SELECT slot FROM slots WHERE slot >= ?1 AND slot < ?2 AND status IN ('done','skipped')",
    )?;
    let rows = stmt.query_map(params![from as i64, to as i64], |r| r.get::<_, i64>(0))?;
    Ok(rows
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .map(|s| s as u64)
        .collect())
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoredStats {
    pub slots: u64,
    pub done: u64,
    pub skipped: u64,
    pub failed: u64,
    pub transactions: u64,
    pub trades: u64,
}

pub fn stored_stats(conn: &Connection) -> Result<StoredStats> {
    let (slots, done, skipped, failed) = conn.query_row(
        "SELECT COUNT(*),
                COALESCE(SUM(status = 'done'), 0),
                COALESCE(SUM(status = 'skipped'), 0),
                COALESCE(SUM(status = 'failed'), 0)
         FROM slots",
        [],
        |r| {
            Ok((
                r.get::<_, i64>(0)? as u64,
                r.get::<_, i64>(1)? as u64,
                r.get::<_, i64>(2)? as u64,
                r.get::<_, i64>(3)? as u64,
            ))
        },
    )?;
    let transactions = conn.query_row("SELECT COUNT(*) FROM transactions", [], |r| {
        r.get::<_, i64>(0).map(|count| count as u64)
    })?;
    let trades = conn.query_row("SELECT COUNT(*) FROM trades", [], |r| {
        r.get::<_, i64>(0).map(|count| count as u64)
    })?;

    Ok(StoredStats {
        slots,
        done,
        skipped,
        failed,
        transactions,
        trades,
    })
}

pub fn write_block(tx: &rusqlite::Transaction<'_>, b: &BlockWrite) -> Result<()> {
    tx.execute(
        "DELETE FROM transactions WHERE slot = ?1",
        params![b.slot as i64],
    )?;
    tx.execute(
        "DELETE FROM account_conflicts WHERE slot = ?1",
        params![b.slot as i64],
    )?;
    tx.execute("DELETE FROM trades WHERE slot = ?1", params![b.slot as i64])?;

    tx.execute(
        "INSERT INTO slots (slot, status, block_time, tx_count, updated_at)
         VALUES (?1, ?2, ?3, ?4, unixepoch())
         ON CONFLICT(slot) DO UPDATE SET
           status = excluded.status,
           block_time = excluded.block_time,
           tx_count = excluded.tx_count,
           updated_at = excluded.updated_at",
        params![
            b.slot as i64,
            b.status.as_str(),
            b.block_time,
            b.transactions.len() as i64
        ],
    )?;

    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO transactions
               (signature, slot, tx_index, succeeded, fee, is_vote, step, program_ids, writable, readonly)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        )?;
        for t in &b.transactions {
            stmt.execute(params![
                t.signature,
                b.slot as i64,
                t.tx_index as i64,
                t.succeeded as i64,
                t.fee as i64,
                t.is_vote as i64,
                t.step as i64,
                t.program_ids,
                t.writable,
                t.readonly,
            ])?;
        }
    }

    if let Some(c) = &b.contention {
        tx.execute(
            "INSERT INTO contention
               (slot, block_time, tx_count, depth, widths, tx_count_novote, depth_novote, widths_novote)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)
             ON CONFLICT(slot) DO UPDATE SET
               block_time = excluded.block_time,
               tx_count = excluded.tx_count,
               depth = excluded.depth,
               widths = excluded.widths,
               tx_count_novote = excluded.tx_count_novote,
               depth_novote = excluded.depth_novote,
               widths_novote = excluded.widths_novote",
            params![
                b.slot as i64,
                b.block_time,
                c.tx_count as i64,
                c.depth as i64,
                c.widths,
                c.tx_count_novote as i64,
                c.depth_novote as i64,
                c.widths_novote
            ],
        )?;
    }

    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO account_conflicts (slot, account, write_locks, read_locks, delays, programs)
             VALUES (?1,?2,?3,?4,?5,?6)",
        )?;
        for c in &b.conflicts {
            stmt.execute(params![
                b.slot as i64,
                c.account,
                c.write_locks,
                c.read_locks,
                c.delays,
                c.programs
            ])?;
        }
    }

    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO trades
               (signature, mint, slot, block_time, tx_index, token_amount, sol_amount, price_sol)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8)
             ON CONFLICT(signature, mint) DO UPDATE SET
               slot = excluded.slot,
               block_time = excluded.block_time,
               tx_index = excluded.tx_index,
               token_amount = excluded.token_amount,
               sol_amount = excluded.sol_amount,
               price_sol = excluded.price_sol",
        )?;
        let bt = b.block_time.unwrap_or(0);
        for t in &b.trades {
            stmt.execute(params![
                t.signature,
                t.mint,
                b.slot as i64,
                bt,
                t.tx_index as i64,
                t.token_amount,
                t.sol_amount,
                t.price_sol
            ])?;
        }
    }

    Ok(())
}

/// Recomputes every candle from the `trades` table.
///
/// Candles are always derived, never incrementally updated, so re-ingesting a
/// slot cannot double-count volume: the trade rows are replaced, then the
/// candles are rebuilt from whatever the table now holds.
pub fn rebuild_candles(conn: &mut Connection) -> Result<usize> {
    let trades = {
        let mut stmt = conn.prepare(
            "SELECT signature, mint, slot, block_time, tx_index, token_amount, sol_amount, price_sol
             FROM trades WHERE block_time > 0 ORDER BY mint, slot, tx_index",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(crate::ohlcv::Trade {
                signature: r.get(0)?,
                mint: r.get(1)?,
                slot: r.get::<_, i64>(2)? as u64,
                block_time: r.get(3)?,
                tx_index: r.get::<_, i64>(4)? as usize,
                token_amount: r.get(5)?,
                sol_amount: r.get(6)?,
                price_sol: r.get(7)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };

    let mut candles = crate::ohlcv::build_candles(&trades, crate::ohlcv::INTERVAL_1M);
    candles.extend(crate::ohlcv::build_candles(
        &trades,
        crate::ohlcv::INTERVAL_5M,
    ));

    let tx = conn.transaction()?;
    tx.execute("DELETE FROM candles", [])?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO candles
               (mint, interval_s, bucket_ts, open, high, low, close, volume_sol, volume_token, trade_count)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
        )?;
        for c in &candles {
            stmt.execute(params![
                c.mint,
                c.interval_s,
                c.bucket_ts,
                c.open,
                c.high,
                c.low,
                c.close,
                c.volume_sol,
                c.volume_token,
                c.trade_count
            ])?;
        }
    }
    tx.commit()?;

    Ok(candles.len())
}


pub struct ReadPool {
    conns: Vec<std::sync::Mutex<Connection>>,
    next: AtomicUsize,
}

impl ReadPool {
    pub fn open(path: &Path, size: usize) -> Result<Arc<Self>> {
        let mut conns = Vec::with_capacity(size);
        for _ in 0..size.max(1) {
            conns.push(std::sync::Mutex::new(open_reader(path)?));
        }
        Ok(Arc::new(Self {
            conns,
            next: AtomicUsize::new(0),
        }))
    }

    pub fn with<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        // Prefer an idle connection; fall back to waiting on the chosen one.
        for i in 0..self.conns.len() {
            let idx = (start + i) % self.conns.len();
            if let Ok(conn) = self.conns[idx].try_lock() {
                return f(&conn);
            }
        }
        let idx = start % self.conns.len();
        let conn = self.conns[idx]
            .lock()
            .map_err(|_| anyhow::anyhow!("read pool poisoned"))?;
        f(&conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(slot: u64) -> BlockWrite {
        BlockWrite {
            slot,
            status: SlotStatus::Done,
            block_time: Some(1_700_000_000),
            transactions: vec![TxRow {
                signature: format!("sig{slot}"),
                tx_index: 0,
                succeeded: true,
                fee: 5000,
                is_vote: false,
                step: 1,
                program_ids: "[\"prog\"]".into(),
                writable: "[\"acc\"]".into(),
                readonly: "[]".into(),
            }],
            contention: Some(ContentionRow {
                tx_count: 1,
                depth: 1,
                widths: "[1]".into(),
                tx_count_novote: 1,
                depth_novote: 1,
                widths_novote: "[1]".into(),
            }),
            conflicts: vec![AccountConflictRow {
                account: "acc".into(),
                write_locks: 1,
                read_locks: 0,
                delays: 0,
                programs: "[\"prog\"]".into(),
            }],
            trades: vec![TradeRow {
                signature: format!("sig{slot}"),
                mint: "mint".into(),
                tx_index: 0,
                token_amount: 10.0,
                sol_amount: 1.0,
                price_sol: 0.1,
            }],
        }
    }

    fn counts(conn: &Connection) -> (i64, i64, i64, i64) {
        let one = |sql: &str| conn.query_row(sql, [], |r| r.get::<_, i64>(0)).unwrap();
        (
            one("SELECT COUNT(*) FROM slots"),
            one("SELECT COUNT(*) FROM transactions"),
            one("SELECT COUNT(*) FROM trades"),
            one("SELECT COUNT(*) FROM account_conflicts"),
        )
    }

    #[test]
    fn writing_the_same_block_twice_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        let mut conn = open_writer(&path).unwrap();

        for _ in 0..3 {
            let tx = conn.transaction().unwrap();
            write_block(&tx, &sample(100)).unwrap();
            tx.commit().unwrap();
        }

        assert_eq!(
            counts(&conn),
            (1, 1, 1, 1),
            "re-writing a slot must not duplicate rows"
        );
    }

    #[test]
    fn rewriting_a_slot_replaces_stale_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        let mut conn = open_writer(&path).unwrap();

        let tx = conn.transaction().unwrap();
        write_block(&tx, &sample(7)).unwrap();
        tx.commit().unwrap();

        // Same slot, but this time it yields no transactions or trades.
        let mut empty = sample(7);
        empty.transactions.clear();
        empty.trades.clear();
        empty.conflicts.clear();
        let tx = conn.transaction().unwrap();
        write_block(&tx, &empty).unwrap();
        tx.commit().unwrap();

        assert_eq!(
            counts(&conn),
            (1, 0, 0, 0),
            "stale per-slot rows should be cleared"
        );
    }

    #[test]
    fn completed_slots_skips_failed_ones() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        let mut conn = open_writer(&path).unwrap();

        for (slot, status) in [
            (1u64, SlotStatus::Done),
            (2, SlotStatus::Skipped),
            (3, SlotStatus::Failed),
        ] {
            let mut b = sample(slot);
            b.status = status;
            let tx = conn.transaction().unwrap();
            write_block(&tx, &b).unwrap();
            tx.commit().unwrap();
        }

        let mut done = completed_slots(&conn, 0, 10).unwrap();
        done.sort();
        assert_eq!(done, vec![1, 2], "failed slots must be retried on a re-run");
    }

    #[test]
    fn stored_stats_describes_an_existing_database() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.db");
        let mut conn = open_writer(&path).unwrap();

        for (slot, status) in [(1, SlotStatus::Done), (2, SlotStatus::Skipped)] {
            let mut block = sample(slot);
            block.status = status;
            if status == SlotStatus::Skipped {
                block.transactions.clear();
                block.trades.clear();
            }
            let tx = conn.transaction().unwrap();
            write_block(&tx, &block).unwrap();
            tx.commit().unwrap();
        }

        assert_eq!(
            stored_stats(&conn).unwrap(),
            StoredStats {
                slots: 2,
                done: 1,
                skipped: 1,
                failed: 0,
                transactions: 1,
                trades: 1,
            }
        );
    }
}
