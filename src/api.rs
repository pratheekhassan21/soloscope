
use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rusqlite::params;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::ohlcv::{INTERVAL_1M, INTERVAL_5M};

type Shared = Arc<AppState>;

pub fn router(state: Shared) -> Router {
    let debug = state.cfg.debug_endpoints;

    let mut app = Router::new()
        .route("/", get(dashboard))
        .route("/app.js", get(app_js))
        .route("/style.css", get(style_css))
        .route("/api/status", get(status))
        .route("/api/contention", get(contention))
        .route("/api/tokens", get(tokens))
        .route("/api/ohlcv", get(ohlcv));

    if debug {
        app = app.route("/api/debug/pause-writer", post(pause_writer));
    }

    app.with_state(state)
}

pub struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(serde_json::json!({ "error": self.1 }))).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    }
}

fn bad_request(msg: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, msg.into())
}

/// Runs a read query off the async runtime.
async fn read<T, F>(state: &Shared, f: F) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce(&rusqlite::Connection) -> anyhow::Result<T> + Send + 'static,
{
    let pool = state.reads.clone();
    tokio::task::spawn_blocking(move || pool.with(f))
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        .map_err(ApiError::from)
}



async fn dashboard() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        include_str!("../static/index.html"),
    )
}

async fn app_js() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        include_str!("../static/app.js"),
    )
}

async fn style_css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../static/style.css"),
    )
}


#[derive(Serialize)]
struct StatusResponse {
    slots_total: u64,
    slots_done: u64,
    slots_skipped: u64,
    slots_failed: u64,
    slots_from_previous_run: u64,
    transactions: u64,
    trades: u64,
    blocks_written: u64,
    write_batches: u64,
    rows_written: u64,
    commit_s: f64,
    fetch_queue: i64,
    write_queue: i64,
    peak_fetch_queue: i64,
    peak_write_queue: i64,
    writer_paused: bool,
    ingest_done: bool,
    elapsed_s: f64,
    slots_per_s: f64,
    exclusions: Vec<ExclusionCount>,
}

#[derive(Serialize)]
struct ExclusionCount {
    reason: String,
    count: u64,
}

async fn status(State(state): State<Shared>) -> Json<StatusResponse> {
    let m = &state.metrics;
    let elapsed = m.elapsed_secs();
    let settled = m.slots_settled();
    let serve_only = state.cfg.serve_only;

    let exclusions = m
        .exclusions
        .lock()
        .map(|e| {
            let mut v: Vec<ExclusionCount> =
                e.0.iter()
                    .map(|(k, c)| ExclusionCount {
                        reason: format!("{k:?}"),
                        count: *c,
                    })
                    .collect();
            v.sort_by_key(|item| std::cmp::Reverse(item.count));
            v
        })
        .unwrap_or_default();

    Json(StatusResponse {
        slots_total: m.slots_total.load(Relaxed),
        slots_done: m.slots_done.load(Relaxed),
        slots_skipped: m.slots_skipped.load(Relaxed),
        slots_failed: m.slots_failed.load(Relaxed),
        slots_from_previous_run: m.slots_already_present.load(Relaxed),
        transactions: m.transactions.load(Relaxed),
        trades: m.trades.load(Relaxed),
        blocks_written: m.blocks_written.load(Relaxed),
        write_batches: m.write_batches.load(Relaxed),
        rows_written: m.rows_written.load(Relaxed),
        commit_s: (m.commit_secs() * 1000.0).round() / 1000.0,
        fetch_queue: m.fetch_queue.load(Relaxed),
        write_queue: m.write_queue.load(Relaxed),
        peak_fetch_queue: m.fetch_queue_max.load(Relaxed),
        peak_write_queue: m.write_queue_max.load(Relaxed),
        writer_paused: m.writer_paused.load(Relaxed),
        ingest_done: m.ingest_done.load(Relaxed),
        elapsed_s: if serve_only {
            0.0
        } else {
            (elapsed * 10.0).round() / 10.0
        },
        slots_per_s: if !serve_only && elapsed > 0.0 {
            (settled as f64 / elapsed * 100.0).round() / 100.0
        } else {
            0.0
        },
        exclusions,
    })
}



#[derive(Deserialize)]
struct RangeQuery {
    from: Option<u64>,
    to: Option<u64>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Serialize)]
struct SlotContention {
    slot: u64,
    block_time: Option<i64>,
    tx_count: usize,
    depth: usize,
    parallelism: f64,
    widths: serde_json::Value,
    tx_count_novote: usize,
    depth_novote: usize,
    parallelism_novote: f64,
}

#[derive(Serialize, Default)]
struct ContentionSummary {
    slots: usize,
    transactions: u64,
    avg_depth: f64,
    max_depth: usize,
    avg_parallelism: f64,
    transactions_novote: u64,
    avg_depth_novote: f64,
    avg_parallelism_novote: f64,
}

#[derive(Serialize)]
struct TopAccount {
    account: String,
    delays: i64,
    write_locks: i64,
    read_locks: i64,
    slots: i64,
}

#[derive(Serialize)]
struct TopProgram {
    program: String,
    delays: i64,
}

#[derive(Serialize)]
struct HistogramBin {
    bucket: String,
    count: i64,
}

#[derive(Serialize)]
struct ContentionResponse {
    from: u64,
    to: u64,
    summary: ContentionSummary,
    depth_histogram: Vec<HistogramBin>,
    slots: Vec<SlotContention>,
    top_accounts: Vec<TopAccount>,
    top_programs: Vec<TopProgram>,
}

async fn contention(
    State(state): State<Shared>,
    Query(q): Query<RangeQuery>,
) -> Result<Json<ContentionResponse>, ApiError> {
    let from = q.from.unwrap_or(0);
    let to = q.to.unwrap_or(u64::MAX >> 1);
    if to < from {
        return Err(bad_request("`to` must not be smaller than `from`"));
    }
    if from > i64::MAX as u64 || to > i64::MAX as u64 {
        return Err(bad_request(
            "slot range exceeds SQLite's signed integer range",
        ));
    }
    let limit = q.limit.unwrap_or(1000).min(5000);

    let resp = read(&state, move |conn| {
        let (lo, hi) = (from as i64, to as i64);

        let summary = conn.query_row(
            "SELECT COUNT(*), COALESCE(SUM(tx_count),0), COALESCE(AVG(depth),0), COALESCE(MAX(depth),0),
                    COALESCE(AVG(CASE WHEN depth > 0 THEN CAST(tx_count AS REAL)/depth END),0),
                    COALESCE(SUM(tx_count_novote),0), COALESCE(AVG(depth_novote),0),
                    COALESCE(AVG(CASE WHEN depth_novote > 0 THEN CAST(tx_count_novote AS REAL)/depth_novote END),0)
             FROM contention WHERE slot >= ?1 AND slot <= ?2",
            params![lo, hi],
            |r| {
                Ok(ContentionSummary {
                    slots: r.get::<_, i64>(0)? as usize,
                    transactions: r.get::<_, i64>(1)? as u64,
                    avg_depth: r.get(2)?,
                    max_depth: r.get::<_, i64>(3)? as usize,
                    avg_parallelism: r.get(4)?,
                    transactions_novote: r.get::<_, i64>(5)? as u64,
                    avg_depth_novote: r.get(6)?,
                    avg_parallelism_novote: r.get(7)?,
                })
            },
        )?;

        let mut stmt = conn.prepare(
            "SELECT slot, block_time, tx_count, depth, widths, tx_count_novote, depth_novote
             FROM contention WHERE slot >= ?1 AND slot <= ?2 ORDER BY slot LIMIT ?3",
        )?;
        let slots = stmt
            .query_map(params![lo, hi, limit as i64], |r| {
                let tx_count: i64 = r.get(2)?;
                let depth: i64 = r.get(3)?;
                let tx_novote: i64 = r.get(5)?;
                let depth_novote: i64 = r.get(6)?;
                let widths: String = r.get(4)?;
                Ok(SlotContention {
                    slot: r.get::<_, i64>(0)? as u64,
                    block_time: r.get(1)?,
                    tx_count: tx_count as usize,
                    depth: depth as usize,
                    parallelism: ratio(tx_count, depth),
                    widths: serde_json::from_str(&widths).unwrap_or(serde_json::Value::Null),
                    tx_count_novote: tx_novote as usize,
                    depth_novote: depth_novote as usize,
                    parallelism_novote: ratio(tx_novote, depth_novote),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let mut stmt = conn.prepare(
            "SELECT CASE
                      WHEN depth <= 5 THEN '1-5'
                      WHEN depth <= 10 THEN '6-10'
                      WHEN depth <= 20 THEN '11-20'
                      WHEN depth <= 50 THEN '21-50'
                      WHEN depth <= 100 THEN '51-100'
                      ELSE '100+' END AS bucket,
                    COUNT(*)
             FROM contention WHERE slot >= ?1 AND slot <= ?2
             GROUP BY bucket ORDER BY MIN(depth)",
        )?;
        let depth_histogram = stmt
            .query_map(params![lo, hi], |r| {
                Ok(HistogramBin { bucket: r.get(0)?, count: r.get(1)? })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        let mut stmt = conn.prepare(
            "SELECT account, SUM(delays), SUM(write_locks), SUM(read_locks), COUNT(DISTINCT slot)
             FROM account_conflicts WHERE slot >= ?1 AND slot <= ?2
             GROUP BY account ORDER BY SUM(delays) DESC, SUM(write_locks) DESC LIMIT 25",
        )?;
        let top_accounts = stmt
            .query_map(params![lo, hi], |r| {
                Ok(TopAccount {
                    account: r.get(0)?,
                    delays: r.get(1)?,
                    write_locks: r.get(2)?,
                    read_locks: r.get(3)?,
                    slots: r.get(4)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        // Programs are stored as a JSON array per conflicting account.
        let top_programs = conn
            .prepare(
                "SELECT je.value AS program, SUM(ac.delays) AS d
                 FROM account_conflicts ac, json_each(ac.programs) je
                 WHERE ac.slot >= ?1 AND ac.slot <= ?2
                 GROUP BY program ORDER BY d DESC LIMIT 25",
            )
            .and_then(|mut stmt| {
                stmt.query_map(params![lo, hi], |r| {
                    Ok(TopProgram { program: r.get(0)?, delays: r.get(1)? })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap_or_default();

        Ok(ContentionResponse {
            from,
            to,
            summary,
            depth_histogram,
            slots,
            top_accounts,
            top_programs,
        })
    })
    .await?;

    Ok(Json(resp))
}

fn ratio(count: i64, depth: i64) -> f64 {
    if depth <= 0 {
        0.0
    } else {
        ((count as f64 / depth as f64) * 1000.0).round() / 1000.0
    }
}



#[derive(Serialize)]
struct TokenRow {
    mint: String,
    trades: i64,
    volume_sol: f64,
    last_price_sol: f64,
    first_seen: i64,
    last_seen: i64,
}

async fn tokens(State(state): State<Shared>) -> Result<Json<Vec<TokenRow>>, ApiError> {
    let rows = read(&state, |conn| {

        let mut stmt = conn.prepare(
            "WITH agg AS (
               SELECT mint, COUNT(*) n, SUM(sol_amount) vol,
                      MIN(block_time) first_seen, MAX(block_time) last_seen
               FROM trades GROUP BY mint
             ),
             latest AS (
               SELECT mint, MAX(bucket_ts) bucket_ts
               FROM candles WHERE interval_s = 60 GROUP BY mint
             )
             SELECT agg.mint, agg.n, agg.vol, agg.first_seen, agg.last_seen,
                    COALESCE(c.close, 0.0)
             FROM agg
             LEFT JOIN latest l ON l.mint = agg.mint
             LEFT JOIN candles c
               ON c.mint = l.mint AND c.interval_s = 60 AND c.bucket_ts = l.bucket_ts
             ORDER BY agg.n DESC",
        )?;
        let rows = stmt
            .query_map([], |r| {
                Ok(TokenRow {
                    mint: r.get(0)?,
                    trades: r.get(1)?,
                    volume_sol: r.get(2)?,
                    first_seen: r.get(3)?,
                    last_seen: r.get(4)?,
                    last_price_sol: r.get(5)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    })
    .await?;

    Ok(Json(rows))
}

// ---------------------------------------------------------------------------
// /api/ohlcv
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct OhlcvQuery {
    mint: String,
    #[serde(default)]
    interval: Option<String>,
}

#[derive(Serialize)]
struct CandleRow {
    bucket_ts: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume_sol: f64,
    volume_token: f64,
    trade_count: i64,
}

#[derive(Serialize)]
struct OhlcvResponse {
    mint: String,
    interval: String,
    candles: Vec<CandleRow>,
}

async fn ohlcv(
    State(state): State<Shared>,
    Query(q): Query<OhlcvQuery>,
) -> Result<Json<OhlcvResponse>, ApiError> {
    let interval = q.interval.unwrap_or_else(|| "1m".into());
    let interval_s = match interval.as_str() {
        "1m" => INTERVAL_1M,
        "5m" => INTERVAL_5M,
        other => {
            return Err(bad_request(format!(
                "interval must be 1m or 5m, got `{other}`"
            )));
        }
    };
    if q.mint.trim().is_empty() {
        return Err(bad_request("mint is required"));
    }

    let mint = q.mint.clone();
    let candles = read(&state, move |conn| {
        let mut stmt = conn.prepare(
            "SELECT bucket_ts, open, high, low, close, volume_sol, volume_token, trade_count
             FROM candles WHERE mint = ?1 AND interval_s = ?2 ORDER BY bucket_ts",
        )?;
        let rows = stmt
            .query_map(params![mint, interval_s], |r| {
                Ok(CandleRow {
                    bucket_ts: r.get(0)?,
                    open: r.get(1)?,
                    high: r.get(2)?,
                    low: r.get(3)?,
                    close: r.get(4)?,
                    volume_sol: r.get(5)?,
                    volume_token: r.get(6)?,
                    trade_count: r.get(7)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    })
    .await?;

    Ok(Json(OhlcvResponse {
        mint: q.mint,
        interval,
        candles,
    }))
}

// ---------------------------------------------------------------------------
// Debug endpoints (opt-in, used by the load experiments)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct PauseQuery {
    #[serde(default)]
    secs: Option<u64>,
}

async fn pause_writer(
    State(state): State<Shared>,
    Query(q): Query<PauseQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let secs = q.secs.unwrap_or(10).min(120);
    crate::ingest::pause_writer(&state.writes, Duration::from_secs(secs)).await?;
    Ok(Json(serde_json::json!({ "paused_for_s": secs })))
}
