use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use solscope::config::Config;
use solscope::metrics::Metrics;
use solscope::{AppState, api, db, ingest, rpc};
use tokio::sync::mpsc;
use tracing_subscriber::EnvFilter;

fn main() -> Result<()> {
    // A .env file is convenient for the RPC URL but must never be required.
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("SOLSCOPE_LOG").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let cfg = Arc::new(Config::parse());

    let mut rt = tokio::runtime::Builder::new_multi_thread();
    rt.enable_all();
    if let Some(n) = cfg.runtime_workers {
        rt.worker_threads(n.max(1));
    }
    rt.build()?.block_on(run(cfg))
}

async fn run(cfg: Arc<Config>) -> Result<()> {
    let metrics = Arc::new(Metrics::default());

    if cfg.serve_only {
        anyhow::ensure!(
            cfg.db.is_file(),
            "serve-only database does not exist: {}",
            cfg.db.display()
        );
    }

    let client = if cfg.serve_only {
        None
    } else {
        let rpc_url = cfg
            .rpc_url
            .clone()
            .context("--rpc-url or SOLSCOPE_RPC_URL is required unless --serve-only is used")?;
        Some(Arc::new(rpc::RpcClient::new(rpc_url, cfg.rps)?))
    };

    // The writer owns the only write connection, and creates the schema, so it
    // must be opened before any reader touches the file.
    let write_conn = db::open_writer(&cfg.db)?;
    let (writes, write_rx) = mpsc::channel::<db::WriteMsg>(cfg.channel_capacity.max(1));

    let writer = {
        let metrics = metrics.clone();
        let batch = cfg.batch_blocks.max(1);
        std::thread::Builder::new()
            .name("db-writer".into())
            .spawn(move || {
                if let Err(e) = ingest::writer_thread(write_conn, write_rx, batch, metrics) {
                    tracing::error!(error = %e, "database writer stopped");
                }
            })?
    };

    let reads = db::ReadPool::open(&cfg.db, 4).context("opening read connections")?;

    if cfg.serve_only {
        let stored = reads.with(db::stored_stats)?;
        metrics
            .slots_total
            .store(stored.slots, std::sync::atomic::Ordering::Relaxed);
        metrics
            .slots_done
            .store(stored.done, std::sync::atomic::Ordering::Relaxed);
        metrics
            .slots_skipped
            .store(stored.skipped, std::sync::atomic::Ordering::Relaxed);
        metrics
            .slots_failed
            .store(stored.failed, std::sync::atomic::Ordering::Relaxed);
        metrics
            .transactions
            .store(stored.transactions, std::sync::atomic::Ordering::Relaxed);
        metrics
            .trades
            .store(stored.trades, std::sync::atomic::Ordering::Relaxed);
    }

    let state = Arc::new(AppState {
        cfg: cfg.clone(),
        metrics: metrics.clone(),
        reads,
        writes: writes.clone(),
    });

    // Serving starts before ingestion, so the API is reachable throughout it.
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", cfg.port))
        .await
        .with_context(|| format!("binding port {}", cfg.port))?;
    tracing::info!(
        url = format!("http://localhost:{}", cfg.port),
        "dashboard ready"
    );

    let server = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, api::router(state)).await {
            tracing::error!(error = %e, "http server stopped");
        }
    });

    if cfg.serve_only {
        metrics
            .ingest_done
            .store(true, std::sync::atomic::Ordering::Relaxed);
        tracing::info!("serve-only mode: skipping ingestion");
    } else {
        ingest::run(
            cfg.clone(),
            client.expect("client is configured outside serve-only mode"),
            writes.clone(),
            metrics.clone(),
        )
        .await?;

        match ingest::rebuild_candles(&writes).await {
            Ok(n) => tracing::info!(candles = n, "candles rebuilt"),
            Err(e) => tracing::error!(error = %e, "candle rebuild failed"),
        }
    }

    if cfg.exit_after_ingest {
        ingest::shutdown_writer(&writes).await;
        drop(writes);
        let _ = writer.join();
        return Ok(());
    }

    tracing::info!("ingestion finished; serving until interrupted (ctrl-c to stop)");
    let _ = tokio::signal::ctrl_c().await;

    server.abort();
    ingest::shutdown_writer(&writes).await;
    drop(writes);
    let _ = writer.join();
    Ok(())
}
