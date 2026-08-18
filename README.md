# solscope

One Rust service that downloads a range of Solana blocks concurrently, stores
them in SQLite, reconstructs how much of each block could have run in parallel,
builds OHLCV candles from transaction metadata, and serves both through an HTTP
API and a dashboard — from a single binary.

## Quick start

```bash
# 1. Point it at any Solana RPC (a free Helius key works).
cp .env.example .env         # then edit SOLSCOPE_RPC_URL

# 2. Build and run. Ingests 1,000 slots, then keeps serving.
cargo build --release
./target/release/solscope

# 3. Open the dashboard.
open http://localhost:8080
```

The API is up before ingestion starts and stays responsive throughout it, so
you can watch the run progress on the dashboard.

Run the tests with `cargo test`. They are all offline; the one test that talks
to a real RPC is `#[ignore]`d and runs via
`cargo test -- --ignored` when `SOLSCOPE_RPC_URL` is set.

### Configuration

`SOLSCOPE_RPC_URL` is read from the environment or a `.env` file; everything
else has a working default. It is not required with `--serve-only`.

| Flag | Default | Purpose |
|---|---|---|
| `--rpc-url` | `$SOLSCOPE_RPC_URL` | JSON-RPC endpoint |
| `--start-slot` | tip − 2000 − slots | First slot; omit for a recent range |
| `--slots` | `1000` | Slots to ingest |
| `--db` | `solscope.db` | SQLite file |
| `--port` | `8080` | HTTP port |
| `--rps` | `10` | Self-imposed upstream request cap |
| `--fetch-workers` | `8` | Concurrent block fetches |
| `--analyze-workers` | `4` | Concurrent block analysers |
| `--fetch-channel-capacity` | `8` | Bounded fetch → analyse channel |
| `--channel-capacity` | `64` | Bounded analyse → writer channel |
| `--batch-blocks` | `32` | Blocks per SQLite write transaction |
| `--serve-only` | off | Serve an existing database, skip ingestion |
| `--exit-after-ingest` | off | Exit when ingestion finishes |
| `--debug-endpoints` | off | Enable `/api/debug/*` (load experiments) |
| `--analyze-on-runtime` | off | Run CPU work on the async runtime (experiment only) |
| `--runtime-workers` | core count | Tokio worker threads (experiment only) |
| `--store-vote-locks` | off | Also persist vote transactions' lock sets |

## The slot range used

**Slots 439,586,292 – 439,587,291** (1,000 consecutive slots) on mainnet-beta,
fetched from Helius's free tier with a self-imposed cap of 10 requests/second.
Reproduce exactly with:

```bash
./target/release/solscope --start-slot 439586292 --slots 1000
```

The range covers roughly seven minutes of chain time, which is what bounds the
candle series: an actively traded token has about seven 1-minute candles.

## API

| Endpoint | Description |
|---|---|
| `GET /` | Dashboard |
| `GET /api/status` | Ingest progress, queue depths, exclusion counts |
| `GET /api/contention?from=&to=` | Per-slot schedules, summary, top contended accounts and programs |
| `GET /api/tokens` | Indexed mints with trade counts and volume |
| `GET /api/ohlcv?mint=&interval=1m\|5m` | Candles for one token |

`POST /api/debug/pause-writer?secs=10` exists only with `--debug-endpoints` and
stalls the database writer, which is how the backpressure experiment is driven.

## Part 1 — Ingest

The pipeline is three stages joined by bounded channels:

```
slot queue ─► fetch workers ─► fetch channel ─► analysers ─► write channel ─► writer thread
              (rate limited)     (cap 8)        (CPU work)     (cap 64)        (SQLite, batched)
```

- **Rate limiting is ours, not the provider's.** A token bucket (capacity 10,
  refilling at 10/s) gates *every* outbound request, retries included. Nothing
  reaches the network without a token.
- **Retries** use exponential backoff with jitter (500 ms base, 30 s cap, 6
  attempts) on HTTP 429/5xx, timeouts, and the transient RPC codes `-32004`
  (block not yet available) and `-32005` (node unhealthy).
- **Skipped slots are not errors.** RPC codes `-32007` and `-32009`, and a null
  result, mark the slot `skipped` and the run continues.
- **Re-runs are safe.** Each slot is recorded in `slots` with a status. A re-run
  fetches only slots that are not already `done` or `skipped`, and every
  per-slot table is cleared for that slot inside the same transaction that
  rewrites it — so re-ingesting replaces rows instead of duplicating them.
  Candles are never incremented in place; they are recomputed from the `trades`
  table, so no amount of re-ingestion can double-count volume.

### v0 transactions and address lookup tables

Requesting blocks with `maxSupportedTransactionVersion: 0` makes the RPC
resolve address lookup tables server-side and return the result in
`meta.loadedAddresses`. A transaction's full account list is therefore

```
message.accountKeys  ++  loadedAddresses.writable  ++  loadedAddresses.readonly
```

in exactly that order, which is the order instruction indices address. Static
keys are split into read and write locks using the message header
(`numRequiredSignatures`, `numReadonlySignedAccounts`,
`numReadonlyUnsignedAccounts`); lookup-table addresses arrive pre-split by the
RPC. No separate lookup-table fetches are needed, and none are performed.

## Part 2 — Contention

### Definitions

- **Conflict.** Transactions A and B conflict when some account is write-locked
  by one and read- *or* write-locked by the other. Two reads of the same account
  do not conflict.
- **Step.** A set of pairwise non-conflicting transactions that could execute
  simultaneously.
- **Schedule.** The ordered list of steps covering a block. Its length is the
  **depth**; the number of transactions in a step is that step's **width**.
- **Parallelism.** Transactions ÷ depth — the mean width, i.e. how much of the
  block could have run at once.

### The algorithm, and why it is a heuristic

Transactions are processed in block order. For each account we track the step of
its most recent writer and the latest step among its readers. A transaction is
placed in the earliest step after every conflicting predecessor:

```
step(T) = 1 + max over T's accounts of
            reads  of a:  last_writer[a]
            writes of a:  max(last_writer[a], max_reader[a])
```

This is the critical path of the conflict DAG induced by block order, computed
in time linear in the number of account references rather than by materialising
the O(n²) conflict graph.

**It is a heuristic, and deliberately so.** It is exact for the problem it
solves — the minimum depth that respects block order — but that is not the same
as the validator's schedule:

- The RPC does not expose the real schedule. Nothing here is a recovery of what
  actually happened; it is a reconstruction of what *could* have happened.
- Block order is preserved. Finding the minimum depth over all permutations is
  graph colouring, which is NP-hard; a reordering scheduler could sometimes do
  better.
- Only *declared* locks are considered, not the accounts a program actually
  touched. Solana's runtime schedules on declared locks too, so this matches
  the real constraint, but it overstates conflicts relative to true data
  dependencies.
- Every transaction is treated as taking equal time. Compute-unit costs, the
  per-account write-lock CU cap, and priority fees are all ignored.
- Failed transactions still take their locks, so they are included.

### Votes are reported separately

Vote transactions are roughly 60% of a block and each one writes only its own
vote account, so they inflate parallelism without contending for anything.
Every metric is therefore reported twice — with and without them. Excluding
votes lowers the parallelism ratio sharply while barely changing depth, which is
itself the finding: votes add width, not depth.

## Part 3 — OHLCV

Prices come only from `preTokenBalances` / `postTokenBalances`,
`preBalances` / `postBalances`, the fee, and block time. No instruction
decoding.

### How a trade is recognised

The **fee payer** is taken as the trader — its balance changes are the trade.
(Summing over every owner would net to zero, since one side's loss is the
other's gain.) For that owner:

1. Net each mint's movement from its token-account deltas, kept as raw integers
   and only divided by `10^decimals` at the end.
2. Compute the SOL leg as the native lamport change **plus the fee added back**
   **plus** any wrapped-SOL movement. WSOL is the same asset as SOL, so wrapping
   cancels out against the native leg automatically.
3. Subtract token-account rent. Opening an SPL token account costs exactly
   0.00203928 SOL and closing one refunds it. This is not part of the trade,
   and leaving it in prices a plain airdrop into a fresh account as though the
   rent had bought the tokens — the single largest source of bad prices found
   while building this (see FINDINGS).
4. Accept it as a trade when **exactly one** non-WSOL mint moved and the SOL leg
   moved the **opposite** way. Then `price = |SOL| / |tokens|`.

### Volume

Volume is the **SOL notional** that changed hands, summed over the bucket, with
token-unit volume recorded alongside it. SOL is the common quote asset here, so
it is the only measure comparable across tokens with different decimals and
supplies; token volume alone would make a 9-decimal memecoin look thousands of
times more active than it is. Each trade is counted once — not once per leg.

### Candles

Buckets are `block_time - (block_time mod interval)` for 60 s and 300 s. Within
a bucket, trades are ordered by `(slot, tx_index)` so open and close follow
on-chain order rather than arrival order. High and low are the extremes of the
trade prices. **Empty buckets are omitted, not forward-filled** — a gap means no
trades, and inventing a flat candle there would be fabricating data.

### What is excluded, and why

Every exclusion is counted and surfaced on the dashboard and `/api/status`.

| Exclusion | Reason |
|---|---|
| `MultiHop` | Two or more non-WSOL mints moved. A routed swap or an LP action; metadata alone cannot attribute the SOL to one pair. |
| `SameDirection` | Token and SOL both entered or both left the wallet — liquidity provision or withdrawal, not an exchange. |
| `WrapOrTransferOnly` | SOL moved but no token did: a wrap, unwrap, or plain transfer. |
| `NoSolLeg` | A token moved but SOL did not. Includes USDC-quoted pairs and, after rent adjustment, transfers into new accounts. These tokens still appear in `/api/tokens`; they simply have no SOL price. |
| `Dust` | Under 0.001 SOL, where rounding and rent dominate the implied price. |
| `Failed` | `meta.err` is set. The transaction reverted, so no value moved. |
| `NoTokenMovement` | Nothing to price. |

Known limitations: the fee payer is assumed to be the trader, which misreads
transactions where a bot pays fees on someone else's behalf; priority fees and
MEV tips paid as plain lamport transfers inflate the SOL leg; and a token whose
only pairs are non-SOL never gets a price.

## Storage

SQLite in WAL mode. A single dedicated OS thread owns the only write connection
and commits in batches; the API reads through a separate pool of read-only
connections, which is what lets queries run while ingestion writes.

Tables: `slots` (per-slot status, for resumable runs), `transactions`,
`contention` (depth and widths per slot), `account_conflicts` (per-slot, per-
account lock counts and delays caused), `trades`, `candles`.

Per-transaction lock sets are persisted for non-vote transactions only.
`--store-vote-locks` keeps them, at a measured cost of about 100 MB per 1,000
slots (~9% of the database) — votes are 61% of rows but only carry ~5 accounts
each, so they are cheap in bytes even though they are numerous. The lock-set
columns as a whole are the dominant cost either way: 468 MB of a 1.0 GB
database, averaging 1.1 KB per non-vote transaction. Contention metrics always
include votes regardless of this setting.

## Layout

```
src/
  main.rs        wiring: writer thread, HTTP server, ingest
  config.rs      CLI flags
  rpc.rs         JSON-RPC, token-bucket rate limiter, retries
  model.rs       wire types, v0 account resolution, lock-set splitting
  ingest.rs      pipeline stages, bounded channels, writer thread
  db.rs          schema, idempotent writes, candle rebuild, read pool
  contention.rs  schedule reconstruction and conflict attribution
  ohlcv.rs       trade inference and candle aggregation
  api.rs         JSON endpoints and static assets
  metrics.rs     counters shared with /api/status
static/          dashboard: plain HTML, CSS, vanilla JS (no build step)
tests/           v0 resolution and idempotent ingestion, over a real block
scripts/         latency and pipeline sampling used by FINDINGS.md
```

Measurements, experiment results, and what did not work are in
[FINDINGS.md](FINDINGS.md).
