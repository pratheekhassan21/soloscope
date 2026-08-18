
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::sync::{Mutex, mpsc};

use crate::config::Config;
use crate::contention;
use crate::db::{
    AccountConflictRow, BlockWrite, ContentionRow, SlotStatus, TradeRow, TxRow, WriteMsg,
};
use crate::metrics::Metrics;
use crate::model::RpcBlock;
use crate::ohlcv;
use crate::rpc::{BlockFetch, RpcClient};

struct RawBlock {
    slot: u64,
    block: Box<RpcBlock>,
}

pub async fn run(
    cfg: Arc<Config>,
    client: Arc<RpcClient>,
    writes: mpsc::Sender<WriteMsg>,
    metrics: Arc<Metrics>,
) -> Result<()> {
    let start_slot = match cfg.start_slot {
        Some(s) => s,
        None => {
            let tip = client.get_slot().await.context("resolving the chain tip")?;
            tip.saturating_sub(cfg.slot_lag).saturating_sub(cfg.slots)
        }
    };
    let end_slot = start_slot
        .checked_add(cfg.slots)
        .context("slot range exceeds u64")?;
    metrics.slots_total.store(cfg.slots, Relaxed);


    let already = {
        let conn = crate::db::open_reader(&cfg.db)?;
        crate::db::completed_slots(&conn, start_slot, end_slot)?
    };
    let done: std::collections::HashSet<u64> = already.into_iter().collect();
    metrics
        .slots_already_present
        .store(done.len() as u64, Relaxed);

    let todo: Vec<u64> = (start_slot..end_slot)
        .filter(|s| !done.contains(s))
        .collect();

    tracing::info!(
        start_slot,
        end_slot,
        total = cfg.slots,
        resuming_from_previous_run = done.len(),
        to_fetch = todo.len(),
        "ingest range selected"
    );

    if todo.is_empty() {
        metrics.ingest_done.store(true, Relaxed);
        return Ok(());
    }

    let (raw_tx, raw_rx) = mpsc::channel::<RawBlock>(cfg.fetch_channel_capacity.max(1));
    let (slot_tx, slot_rx) = mpsc::channel::<u64>(cfg.fetch_workers.max(1) * 2);
    let slot_rx = Arc::new(Mutex::new(slot_rx));


    let raw_rx = Arc::new(Mutex::new(raw_rx));
    let mut analysers = Vec::new();
    for _ in 0..cfg.analyze_workers.max(1) {
        analysers.push(tokio::spawn(analyse_worker(
            cfg.clone(),
            raw_rx.clone(),
            writes.clone(),
            metrics.clone(),
        )));
    }

   
    let mut fetchers = Vec::new();
    for _ in 0..cfg.fetch_workers.max(1) {
        fetchers.push(tokio::spawn(fetch_worker(
            client.clone(),
            slot_rx.clone(),
            raw_tx.clone(),
            writes.clone(),
            metrics.clone(),
        )));
    }
    drop(raw_tx);

    let started = Instant::now();

    // Feeding the queue blocks once the workers are saturated, which is the
    // point at which backpressure has reached the top of the pipeline.
    for slot in todo {
        if slot_tx.send(slot).await.is_err() {
            break;
        }
    }
    drop(slot_tx);

    for f in fetchers {
        let _ = f.await;
    }
    for a in analysers {
        let _ = a.await;
    }

    metrics.ingest_done.store(true, Relaxed);

    let elapsed = started.elapsed().as_secs_f64();
    let settled = metrics.slots_settled();
    tracing::info!(
        slots_done = metrics.slots_done.load(Relaxed),
        slots_skipped = metrics.slots_skipped.load(Relaxed),
        slots_failed = metrics.slots_failed.load(Relaxed),
        transactions = metrics.transactions.load(Relaxed),
        trades = metrics.trades.load(Relaxed),
        rpc_requests = client.request_count(),
        elapsed_s = format!("{elapsed:.1}"),
        slots_per_s = format!("{:.1}", settled as f64 / elapsed.max(1e-9)),
        effective_rps = format!("{:.2}", client.request_count() as f64 / elapsed.max(1e-9)),
        peak_fetch_queue = metrics.fetch_queue_max.load(Relaxed),
        peak_write_queue = metrics.write_queue_max.load(Relaxed),
        write_batches = metrics.write_batches.load(Relaxed),
        rows_written = metrics.rows_written.load(Relaxed),
        commit_s = format!("{:.2}", metrics.commit_secs()),
        commit_pct = format!("{:.1}", 100.0 * metrics.commit_secs() / elapsed.max(1e-9)),
        rows_per_commit_s = format!(
            "{:.0}",
            metrics.rows_written.load(Relaxed) as f64 / metrics.commit_secs().max(1e-9)
        ),
        "ingest complete"
    );

    Ok(())
}

async fn fetch_worker(
    client: Arc<RpcClient>,
    slots: Arc<Mutex<mpsc::Receiver<u64>>>,
    raw: mpsc::Sender<RawBlock>,
    writes: mpsc::Sender<WriteMsg>,
    metrics: Arc<Metrics>,
) {
    loop {
        let Some(slot) = slots.lock().await.recv().await else {
            return;
        };

        match client.get_block(slot).await {
            Ok(BlockFetch::Block(block)) => {
                metrics.enter_fetch_queue();
                // Blocks here when the analysers are behind: the fetcher stops
                // consuming rate-limit tokens rather than piling blocks up.
                if raw.send(RawBlock { slot, block }).await.is_err() {
                    metrics.leave_fetch_queue();
                    return;
                }
            }
            Ok(BlockFetch::Skipped) => {
                // Skipped slots are normal on Solana, not failures.
                metrics.slots_skipped.fetch_add(1, Relaxed);
                metrics.enter_write_queue();
                if writes
                    .send(WriteMsg::Block(Box::new(BlockWrite::skipped(slot))))
                    .await
                    .is_err()
                {
                    metrics.leave_write_queue();
                    return;
                }
            }
            Err(e) => {
                tracing::warn!(slot, error = %e, "slot failed after retries");
                metrics.slots_failed.fetch_add(1, Relaxed);
                metrics.enter_write_queue();
                if writes
                    .send(WriteMsg::Block(Box::new(BlockWrite::failed(slot))))
                    .await
                    .is_err()
                {
                    metrics.leave_write_queue();
                    return;
                }
            }
        }
    }
}

async fn analyse_worker(
    cfg: Arc<Config>,
    raw: Arc<Mutex<mpsc::Receiver<RawBlock>>>,
    writes: mpsc::Sender<WriteMsg>,
    metrics: Arc<Metrics>,
) {
    loop {
        let Some(RawBlock { slot, block }) = raw.lock().await.recv().await else {
            return;
        };
        metrics.leave_fetch_queue();

        // Scheduling and trade inference are CPU-bound. Doing them on the async
        // runtime stalls every other task on that worker thread, including the
        // API; --analyze-on-runtime exists only to demonstrate that.
        let store_vote_locks = cfg.store_vote_locks;
        let (write, exclusions) = if cfg.analyze_on_runtime {
            analyse(slot, &block, store_vote_locks)
        } else {
            match tokio::task::spawn_blocking(move || analyse(slot, &block, store_vote_locks)).await
            {
                Ok(v) => v,
                Err(e) => {
                    tracing::error!(slot, error = %e, "analysis task panicked");
                    continue;
                }
            }
        };

        metrics
            .transactions
            .fetch_add(write.transactions.len() as u64, Relaxed);
        metrics.trades.fetch_add(write.trades.len() as u64, Relaxed);
        metrics.slots_done.fetch_add(1, Relaxed);
        metrics.record_exclusions(&exclusions);

        metrics.enter_write_queue();
        if writes.send(WriteMsg::Block(Box::new(write))).await.is_err() {
            metrics.leave_write_queue();
            return;
        }
    }
}

fn json_keys(keys: &[String], is_vote: bool, store_vote_locks: bool) -> String {
    if is_vote && !store_vote_locks {
        return "[]".to_string();
    }
    serde_json::to_string(keys).unwrap_or_else(|_| "[]".into())
}

/// Pure CPU stage: resolve locks, schedule the block, infer trades.
pub fn analyse(
    slot: u64,
    block: &RpcBlock,
    store_vote_locks: bool,
) -> (BlockWrite, ohlcv::Exclusions) {
    let resolved = block.resolve(slot);
    let analysis = contention::analyze(&resolved);
    let (trades, exclusions) = ohlcv::infer_block_trades(slot, block);

    let transactions = resolved
        .transactions
        .iter()
        .enumerate()
        .map(|(i, t)| TxRow {
            signature: t.signature.clone(),
            tx_index: t.index,
            succeeded: t.succeeded,
            fee: t.fee,
            is_vote: t.is_vote,
            step: analysis.all.steps.get(i).copied().unwrap_or(0),
            program_ids: serde_json::to_string(&t.program_ids).unwrap_or_else(|_| "[]".into()),
            // A vote's lock set is its own vote account plus two sysvars, and
            // votes are the majority of rows. Keeping them would roughly treble
            // the database for data nothing queries.
            writable: json_keys(&t.writable, t.is_vote, store_vote_locks),
            readonly: json_keys(&t.readonly, t.is_vote, store_vote_locks),
        })
        .collect();

    let conflicts = analysis
        .conflicts
        .iter()
        .map(|c| AccountConflictRow {
            account: c.account.clone(),
            write_locks: c.write_locks,
            read_locks: c.read_locks,
            delays: c.delays,
            programs: serde_json::to_string(&c.programs).unwrap_or_else(|_| "[]".into()),
        })
        .collect();

    let contention_row = ContentionRow {
        tx_count: analysis.all.tx_count(),
        depth: analysis.all.depth,
        widths: serde_json::to_string(&analysis.all.widths).unwrap_or_else(|_| "[]".into()),
        tx_count_novote: analysis.non_vote.tx_count(),
        depth_novote: analysis.non_vote.depth,
        widths_novote: serde_json::to_string(&analysis.non_vote.widths)
            .unwrap_or_else(|_| "[]".into()),
    };

    let trade_rows = trades
        .into_iter()
        .map(|t| TradeRow {
            signature: t.signature,
            mint: t.mint,
            tx_index: t.tx_index,
            token_amount: t.token_amount,
            sol_amount: t.sol_amount,
            price_sol: t.price_sol,
        })
        .collect();

    (
        BlockWrite {
            slot,
            status: SlotStatus::Done,
            block_time: resolved.block_time,
            transactions,
            contention: Some(contention_row),
            conflicts,
            trades: trade_rows,
        },
        exclusions,
    )
}

/// The database writer. Runs on its own OS thread with the only write
/// connection, committing blocks in batches to amortise transaction overhead.
/// With the default WAL + synchronous=NORMAL settings, commits do not each
/// force an fsync; the write-path benchmark measures the actual effect.
pub fn writer_thread(
    mut conn: rusqlite::Connection,
    mut rx: mpsc::Receiver<WriteMsg>,
    batch_blocks: usize,
    metrics: Arc<Metrics>,
) -> Result<()> {
    let mut pending: Vec<Box<BlockWrite>> = Vec::with_capacity(batch_blocks);

    loop {
        let Some(msg) = rx.blocking_recv() else { break };

        let mut deferred: Option<WriteMsg> = None;
        match msg {
            WriteMsg::Block(b) => {
                metrics.leave_write_queue();
                pending.push(b);
                // Take whatever else is already queued, up to the batch size.
                while pending.len() < batch_blocks {
                    match rx.try_recv() {
                        Ok(WriteMsg::Block(b)) => {
                            metrics.leave_write_queue();
                            pending.push(b);
                        }
                        Ok(other) => {
                            deferred = Some(other);
                            break;
                        }
                        Err(_) => break,
                    }
                }
            }
            other => deferred = Some(other),
        }

        if !pending.is_empty() {
            commit(&mut conn, &pending, &metrics)?;
            pending.clear();
        }

        match deferred {
            Some(WriteMsg::Pause(d)) => {
                tracing::warn!(seconds = d.as_secs_f64(), "writer paused (experiment)");
                metrics.writer_paused.store(true, Relaxed);
                std::thread::sleep(d);
                metrics.writer_paused.store(false, Relaxed);
                tracing::warn!("writer resumed");
            }
            Some(WriteMsg::RebuildCandles(reply)) => {
                let result = crate::db::rebuild_candles(&mut conn).map_err(|e| e.to_string());
                let _ = reply.send(result);
            }
            Some(WriteMsg::Shutdown(reply)) => {
                let _ = reply.send(());
                break;
            }
            Some(WriteMsg::Block(_)) | None => {}
        }
    }

    if !pending.is_empty() {
        commit(&mut conn, &pending, &metrics)?;
    }
    Ok(())
}

fn commit(
    conn: &mut rusqlite::Connection,
    batch: &[Box<BlockWrite>],
    metrics: &Metrics,
) -> Result<()> {
    let rows: usize = batch
        .iter()
        .map(|b| b.transactions.len() + b.trades.len() + b.conflicts.len() + 1)
        .sum();

    let started = std::time::Instant::now();
    let tx = conn.transaction()?;
    for b in batch {
        crate::db::write_block(&tx, b)?;
    }
    tx.commit()?;
    let elapsed = started.elapsed();

    metrics
        .blocks_written
        .fetch_add(batch.len() as u64, Relaxed);
    metrics.write_batches.fetch_add(1, Relaxed);
    metrics.rows_written.fetch_add(rows as u64, Relaxed);
    metrics
        .commit_nanos
        .fetch_add(elapsed.as_nanos() as u64, Relaxed);
    Ok(())
}

pub async fn rebuild_candles(writes: &mpsc::Sender<WriteMsg>) -> Result<usize> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    writes
        .send(WriteMsg::RebuildCandles(tx))
        .await
        .map_err(|_| anyhow::anyhow!("writer is gone"))?;
    rx.await
        .map_err(|_| anyhow::anyhow!("writer dropped the reply"))?
        .map_err(|e| anyhow::anyhow!(e))
}


pub async fn pause_writer(writes: &mpsc::Sender<WriteMsg>, d: Duration) -> Result<()> {
    writes
        .send(WriteMsg::Pause(d))
        .await
        .map_err(|_| anyhow::anyhow!("writer is gone"))
}

pub async fn shutdown_writer(writes: &mpsc::Sender<WriteMsg>) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    if writes.send(WriteMsg::Shutdown(tx)).await.is_ok() {
        let _ = rx.await;
    }
}
