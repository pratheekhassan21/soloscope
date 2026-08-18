

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rand::RngExt;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep};

use crate::model::RpcBlock;

/// Slot was skipped, or is no longer served by this node. Not an error.
const SLOT_SKIPPED: i64 = -32007;
const SLOT_NOT_IN_LONG_TERM_STORAGE: i64 = -32009;
/// Block not available for this slot yet; worth retrying.
const BLOCK_NOT_AVAILABLE: i64 = -32004;
const NODE_UNHEALTHY: i64 = -32005;

#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("invalid RPC configuration: {0}")]
    InvalidConfig(String),
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("malformed response: {0}")]
    Decode(String),
    #[error("rpc error {code}: {message}")]
    Rpc { code: i64, message: String },
    #[error("giving up after {attempts} attempts: {last}")]
    Exhausted { attempts: u32, last: String },
}

#[derive(Debug)]
pub enum BlockFetch {
    Block(Box<RpcBlock>),
    /// The slot produced no block. Normal on Solana.
    Skipped,
}

struct RateLimiter {
    inner: Mutex<Bucket>,
    capacity: f64,
    refill_per_sec: f64,
}

struct Bucket {
    tokens: f64,
    last: Instant,
}

impl RateLimiter {
    fn new(rps: f64) -> Self {
        let capacity = rps.max(1.0);
        Self {
            inner: Mutex::new(Bucket {
                tokens: capacity,
                last: Instant::now(),
            }),
            capacity,
            refill_per_sec: rps,
        }
    }

    /// Waits until a token is available, then consumes it.
    pub async fn acquire(&self) {
        loop {
            let wait = {
                let mut b = self.inner.lock().await;
                let now = Instant::now();
                let elapsed = now.duration_since(b.last).as_secs_f64();
                b.tokens = (b.tokens + elapsed * self.refill_per_sec).min(self.capacity);
                b.last = now;

                if b.tokens >= 1.0 {
                    b.tokens -= 1.0;
                    return;
                }
                // Sleep only as long as the next token needs, holding no lock.
                Duration::from_secs_f64((1.0 - b.tokens) / self.refill_per_sec)
            };
            sleep(wait).await;
        }
    }
}



pub struct RpcClient {
    http: reqwest::Client,
    url: String,
    limiter: Arc<RateLimiter>,
    max_attempts: u32,
    /// Total upstream requests issued, for the throughput numbers in FINDINGS.
    pub requests: AtomicU64,
}

impl RpcClient {
    pub fn new(url: String, rps: f64) -> Result<Self, RpcError> {
        if !rps.is_finite() || rps <= 0.0 {
            return Err(RpcError::InvalidConfig(
                "requests per second must be a finite positive number".into(),
            ));
        }
        let parsed = reqwest::Url::parse(&url)
            .map_err(|e| RpcError::InvalidConfig(format!("invalid RPC URL: {e}")))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(RpcError::InvalidConfig(
                "RPC URL must use http or https".into(),
            ));
        }
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(16)
            .build()?;
        Ok(Self {
            http,
            url,
            limiter: Arc::new(RateLimiter::new(rps)),
            max_attempts: 6,
            requests: AtomicU64::new(0),
        })
    }

    pub fn request_count(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    /// One rate-limited JSON-RPC round trip. No retries at this level.
    async fn call_once(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<JsonRpcResponse, RpcError> {
        self.limiter.acquire().await;
        self.requests.fetch_add(1, Ordering::Relaxed);

        let resp = self
            .http
            .post(&self.url)
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
            .send()
            .await?;

        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            let retry_after = resp
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());
            return Err(RpcError::Rpc {
                code: -(status.as_u16() as i64),
                message: match retry_after {
                    Some(s) => format!("http {status}, retry-after {s}s"),
                    None => format!("http {status}"),
                },
            });
        }
        if !status.is_success() {
            return Err(RpcError::Decode(format!("http {status}")));
        }

        let body = resp.bytes().await?;
        serde_json::from_slice::<JsonRpcResponse>(&body)
            .map_err(|e| RpcError::Decode(format!("{e}")))
    }

    /// Rate-limited call with exponential backoff and jitter on transient failures.
    async fn call_retrying(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, RpcError> {
        let mut last = String::new();

        for attempt in 0..self.max_attempts {
            if attempt > 0 {
                sleep(backoff_delay(attempt)).await;
            }

            match self.call_once(method, params.clone()).await {
                Ok(resp) => {
                    if let Some(err) = resp.error {
                        // Permanent for this slot: report upward without retrying.
                        if matches!(err.code, SLOT_SKIPPED | SLOT_NOT_IN_LONG_TERM_STORAGE) {
                            return Err(RpcError::Rpc {
                                code: err.code,
                                message: err.message,
                            });
                        }
                        if !matches!(err.code, BLOCK_NOT_AVAILABLE | NODE_UNHEALTHY) {
                            return Err(RpcError::Rpc {
                                code: err.code,
                                message: err.message,
                            });
                        }
                        last = format!("rpc {}: {}", err.code, err.message);
                        continue;
                    }
                    return resp.result.ok_or_else(|| {
                        RpcError::Decode("response had neither result nor error".into())
                    });
                }
                Err(e @ (RpcError::Transport(_) | RpcError::Rpc { .. })) => {
                    last = e.to_string();
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(RpcError::Exhausted {
            attempts: self.max_attempts,
            last,
        })
    }

    pub async fn get_slot(&self) -> Result<u64, RpcError> {
        let v = self
            .call_retrying("getSlot", json!([{"commitment": "finalized"}]))
            .await?;
        v.as_u64()
            .ok_or_else(|| RpcError::Decode("getSlot did not return a number".into()))
    }

    pub async fn get_block(&self, slot: u64) -> Result<BlockFetch, RpcError> {
        let params = json!([
            slot,
            {
                "encoding": "json",
                "transactionDetails": "full",
                "maxSupportedTransactionVersion": 0,
                "rewards": false,
            }
        ]);

        match self.call_retrying("getBlock", params).await {
            Ok(v) => {
                if v.is_null() {
                    return Ok(BlockFetch::Skipped);
                }
                let block: RpcBlock =
                    serde_json::from_value(v).map_err(|e| RpcError::Decode(e.to_string()))?;
                Ok(BlockFetch::Block(Box::new(block)))
            }
            Err(RpcError::Rpc {
                code: SLOT_SKIPPED | SLOT_NOT_IN_LONG_TERM_STORAGE,
                ..
            }) => Ok(BlockFetch::Skipped),
            Err(e) => Err(e),
        }
    }
}

/// Exponential backoff, capped, with jitter to avoid retry convoys across workers.
fn backoff_delay(attempt: u32) -> Duration {
    const BASE_MS: u64 = 500;
    const CAP_MS: u64 = 30_000;
    let exp = BASE_MS.saturating_mul(1 << attempt.min(6)).min(CAP_MS);
    let jitter = rand::rng().random_range(0..=exp / 2);
    Duration::from_millis(exp / 2 + jitter)
}

#[derive(Debug, Deserialize)]
struct JsonRpcResponse {
    #[serde(default)]
    result: Option<serde_json::Value>,
    #[serde(default)]
    error: Option<JsonRpcErrorBody>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcErrorBody {
    code: i64,
    #[serde(default)]
    message: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn limiter_allows_a_burst_then_throttles_to_the_cap() {
        let limiter = RateLimiter::new(10.0);
        let start = Instant::now();

        // The bucket starts full, so the first 10 are immediate.
        for _ in 0..10 {
            limiter.acquire().await;
        }
        assert_eq!(
            start.elapsed(),
            Duration::ZERO,
            "initial burst should not wait"
        );

        // The 11th must wait for a refill: 1 token at 10/s = 100ms.
        limiter.acquire().await;
        assert!(
            start.elapsed() >= Duration::from_millis(100),
            "expected throttling after the burst, waited {:?}",
            start.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn limiter_sustains_the_configured_rate() {
        let limiter = RateLimiter::new(10.0);
        let start = Instant::now();

        // 30 requests: 10 burst + 20 more at 10/s = ~2s.
        for _ in 0..30 {
            limiter.acquire().await;
        }

        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(1900) && elapsed <= Duration::from_millis(2200),
            "30 requests at 10rps should take ~2s, took {elapsed:?}"
        );
    }

    #[test]
    fn backoff_grows_and_stays_capped() {
        let first = backoff_delay(1);
        let later = backoff_delay(5);
        assert!(first < Duration::from_secs(2));
        assert!(later > first);
        for attempt in 0..12 {
            assert!(backoff_delay(attempt) <= Duration::from_secs(30));
        }
    }


    #[tokio::test]
    #[ignore = "requires network and SOLSCOPE_RPC_URL"]
    async fn fetches_a_real_block() {
        let _ = dotenvy::dotenv();
        let url = std::env::var("SOLSCOPE_RPC_URL").expect("SOLSCOPE_RPC_URL");
        let client = RpcClient::new(url, 10.0).unwrap();

        let tip = client.get_slot().await.expect("getSlot");
        assert!(tip > 0);

        // Step back from the tip so the block is certainly finalized.
        let mut found = None;
        for slot in (tip - 2000..tip - 1990).rev() {
            if let BlockFetch::Block(b) = client.get_block(slot).await.expect("getBlock") {
                found = Some((slot, b));
                break;
            }
        }

        let (slot, block) = found.expect("at least one of ten slots should have a block");
        assert!(!block.transactions.is_empty());

        let resolved = block.resolve(slot);
        let v0_with_lookups = resolved
            .transactions
            .iter()
            .find(|t| !t.writable.is_empty() && !t.readonly.is_empty());
        assert!(v0_with_lookups.is_some(), "expected resolved lock sets");
        assert!(
            resolved.transactions.iter().any(|t| t.is_vote),
            "a real block should contain vote transactions"
        );
    }

    #[test]
    fn skipped_slot_codes_are_recognised() {
        // Guards against typos in the constants the fetch path branches on.
        assert_eq!(SLOT_SKIPPED, -32007);
        assert_eq!(SLOT_NOT_IN_LONG_TERM_STORAGE, -32009);
        assert_eq!(BLOCK_NOT_AVAILABLE, -32004);
    }

    #[test]
    fn rejects_invalid_rate_limits() {
        for rps in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(matches!(
                RpcClient::new("http://localhost".into(), rps),
                Err(RpcError::InvalidConfig(_))
            ));
        }
    }

    #[test]
    fn rejects_invalid_rpc_urls() {
        for url in ["", "not a URL", "file:///tmp/rpc"] {
            assert!(matches!(
                RpcClient::new(url.into(), 10.0),
                Err(RpcError::InvalidConfig(_))
            ));
        }
    }
}
