# Findings

This report separates observations from the 1,000-slot reference run from
controlled local experiments. The reference range is reproducible with the
command in the README; the microbenchmarks use real `analyse` and SQLite write
code but repeat the checked-in block fixture so network variance is removed.

## Reference dataset

Range `439,586,292..=439,587,291` covers 2026-08-16 06:06:07–06:13:09 UTC.
All 1,000 requested slots were stored successfully.

| Measure | Result |
|---|---:|
| Transactions | 1,120,417 |
| Vote transactions | 684,090 (61.1%) |
| Failed transactions | 121,831 (10.9%) |
| Inferred SOL-quoted trades | 82,104 |
| Distinct priced mints | 1,353 |
| Inferred SOL notional | 179,968.42 SOL |
| 1-minute / 5-minute candles | 3,679 / 1,868 |

The trade total is intentionally much lower than the transaction total. The
classifier accepts only a single non-WSOL mint moving opposite a SOL leg for
the fee payer. Votes, failed transactions, transfers, non-SOL pairs, routed
swaps, liquidity actions, and dust are excluded. Exclusion counters are
available live through `/api/status`; they are currently run-local rather than
persisted, so they cannot be reconstructed from this database alone.

## Contention

| Per-slot mean | All transactions | Excluding votes |
|---|---:|---:|
| Transactions | 1,120.4 | 436.3 |
| Schedule depth | 112.65 | 112.58 |
| Parallelism (`tx / depth`) | 12.90× | 4.66× |

Removing votes cuts the transaction count by 61% but changes mean depth by
only 0.07 steps. In other words, votes add width and almost no critical-path
length. This is why a single parallelism number that includes votes gives an
overly optimistic picture of useful application concurrency.

Depth is not uniformly small: 483 of 1,000 blocks have depth above 100 and the
maximum is 506. The most delay-associated accounts recur across hundreds of
slots; wrapped SOL itself ranks seventh, with 14,917 attributed delays across
735 slots. At program level, the strongest associations are the compute-budget,
system, associated-token, and SPL-token programs. These are associations with
transactions delayed on hot accounts, not proof that the named program caused
the conflict.

The schedule is the critical path of the conflict DAG while preserving block
order. It is exact for that stated model, but it is not the validator's hidden
runtime schedule and it is not optimal over transaction reorderings. The
README describes the assumptions in detail.

## OHLCV observations

The busiest mint has 4,314 inferred trades and 2,777.27 SOL of notional in the
seven-minute window. Across all mints, 82,104 accepted trades produce only
3,679 non-empty one-minute candles. Sparse series are expected: empty buckets
are deliberately omitted instead of fabricating flat candles.

The largest correctness improvement was removing token-account rent from the
fee payer's native balance change. Without that adjustment, receiving tokens
into a newly created associated token account can look like a purchase priced
at exactly the rent-exempt reserve. Fees are similarly added back, and wrapped
SOL movement is combined with native SOL so wrapping does not become volume.

Metadata-only inference still cannot allocate a SOL leg across multiple mint
movements, distinguish a sponsored fee payer, or remove arbitrary MEV tips.
Those cases are excluded where detectable and documented rather than silently
treated as exact trades.

## Storage

The reference SQLite database is 1.04 GiB. Its largest objects are:

| Object | Size |
|---|---:|
| `transactions` table | 869.8 MiB |
| transaction-signature index | 119.4 MiB |
| `trades` table | 14.6 MiB |
| trade primary-key index | 13.0 MiB |
| `account_conflicts` table | 9.8 MiB |

The two JSON lock-set columns contain 467.9 MiB of payload, averaging 1,118
bytes per non-vote transaction. Vote lock sets are omitted by default and take
only the four bytes needed for two empty arrays. This retains all contention
results while avoiding storage for the least informative 61% of transactions.

## Pipeline experiments

Measurements below were taken on an Apple M4 MacBook Air with 16 GiB RAM,
macOS 26.5, Rust 1.97.1, and SQLite 3.51.2. They are directional results, not
portable capacity guarantees.

### Backpressure and batching

The live run is upstream-bound: the writer usually receives one block at a
time, so `--batch-blocks` is a ceiling rather than a delay waiting for a full
batch. A controlled backlog of 200 realistic 1,104-transaction blocks produced
318,000 inserted rows per pass:

| Batch | Commits | WAL/NORMAL rows/s | WAL/FULL rows/s |
|---:|---:|---:|---:|
| 1 | 200 | 248,423 | 263,371 |
| 8 | 25 | 239,706 | 248,968 |
| 32 | 7 | 248,870 | 252,696 |
| 128 | 2 | 247,425 | 252,119 |
| 512 | 1 | 257,566 | 260,649 |

Batch size is effectively flat on this hardware under both synchronous modes;
the cost is dominated by row and index work, not transaction boundaries. The
default of 32 remains a reasonable upper bound, but tuning it is not a useful
optimization for this workload.

Rollback-journal mode wrote faster in isolation (420,923 rows/s versus 251,051
for WAL), but it blocked the concurrent reader: read p95 was 796 ms and only
three reads completed during the write pass. WAL sustained 487 reads with
0.023 ms p50 and 0.032 ms p95. WAL is therefore the correct tradeoff for a
service that must remain queryable during ingestion.

The bounded-channel policy was also exercised by pausing the writer. The write
queue fills to its configured capacity, analysis then blocks, the fetch queue
fills, and fetch workers stop taking rate-limit tokens. No unbounded block
buffer is created.

### Async starvation

Forty real analysis passes took 0.29 seconds in either placement. With one
Tokio worker, however, running analysis directly on the async runtime delayed
a 1 ms responsiveness probe to 14.28 ms p99. `spawn_blocking` reduced p99 to
1.72 ms without changing analysis throughput. That is why the normal pipeline
keeps analysis off the async executor; `--analyze-on-runtime` exists only to
reproduce the failure mode.

## API latency and query profile

At rest on the full database:

| Endpoint/load | Throughput | p50 | p95 | p99 |
|---|---:|---:|---:|---:|
| `/api/status`, 8 clients | 10,754 req/s | 0.70 ms | 1.21 ms | 1.48 ms |
| `/api/tokens`, 1 client | 133 req/s | 7.47 ms | 7.70 ms | 7.84 ms |
| `/api/tokens`, 8 clients | 375 req/s | 21.17 ms | 31.90 ms | 32.57 ms |
| full-range `/api/contention`, 1 client | 16 req/s | 63.12 ms | 64.57 ms | 76.66 ms |

During a fresh live ingestion, the same eight-client status load measured 0.72
ms p50 and 1.25 ms p95 with no errors. Token-list p95 was 1.55 ms and a
then-partial contention query p95 was 1.96 ms. Those latter two values are not
directly comparable to the full database, but they verify that reads continue
while writes commit.

`/api/tokens` was initially the clear query bottleneck at roughly 337 ms p50.
Its last-price expression ran a correlated, sorted `trades` subquery for every
mint. Sourcing close from the already aggregated one-minute candle table cut
that to roughly 172 ms. Adding the covering
`trades(mint, sol_amount, block_time)` index removed a table lookup for every
trade aggregate, and replacing the candle window with a grouped latest-bucket
join brought the final endpoint to 7.47 ms p50. On warm direct SQL runs, the
old correlated shape takes about 63 ms and the final shape about 8 ms.

## Reproducing the experiments

```bash
cargo build --release
cargo run --release --example starvation
cargo run --release --example writepath

# Live experiments use SOLSCOPE_RPC_URL and throwaway databases.
./scripts/run_experiments.sh experiments

# Measure any running endpoint.
./scripts/api_latency.py \
  --url http://localhost:8080/api/tokens --seconds 20 --concurrency 8
```

Absolute timings will vary with the RPC provider, filesystem, and machine. The
checked-in tests are offline and verify the scheduling, v0 lookup resolution,
trade inference, candle ordering, and idempotent replacement invariants.
