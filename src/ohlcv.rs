
use std::collections::{HashMap, HashSet};

use crate::model::{LAMPORTS_PER_SOL, RpcBlock, RpcTransaction, WSOL_MINT};


pub const DUST_SOL: f64 = 0.001;


pub const TOKEN_ACCOUNT_RENT_LAMPORTS: i128 = 2_039_280;

pub const INTERVAL_1M: i64 = 60;
pub const INTERVAL_5M: i64 = 300;

#[derive(Debug, Clone, PartialEq)]
pub struct Trade {
    pub signature: String,
    pub mint: String,
    pub slot: u64,
    pub tx_index: usize,
    pub block_time: i64,
    /// Absolute token amount, in whole tokens (decimals applied).
    pub token_amount: f64,
    /// Absolute SOL amount on the other side of the trade.
    pub sol_amount: f64,
    /// SOL per whole token.
    pub price_sol: f64,
}

/// Why a transaction produced no trade. Counted so FINDINGS can quantify what
/// the model throws away.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Excluded {
    Failed,
    Vote,
    NoBlockTime,
    /// No SPL token balances changed for the fee payer.
    NoTokenMovement,
    /// Two or more non-WSOL mints moved: a multi-hop route or an LP action,
    /// which metadata alone cannot attribute to a single pair.
    MultiHop,
    /// Token moved but SOL did not: the pair is not quoted in SOL here.
    NoSolLeg,
    /// Token and SOL moved the same way, so this adds or removes liquidity
    /// rather than exchanging one for the other.
    SameDirection,
    /// SOL moved but no token did: a wrap, unwrap or plain transfer.
    WrapOrTransferOnly,
    /// Below the dust threshold.
    Dust,
}

#[derive(Debug, Clone, Default)]
pub struct Exclusions(pub HashMap<Excluded, u64>);

impl Exclusions {
    fn bump(&mut self, reason: Excluded) {
        *self.0.entry(reason).or_insert(0) += 1;
    }

    pub fn get(&self, reason: Excluded) -> u64 {
        self.0.get(&reason).copied().unwrap_or(0)
    }

    pub fn total(&self) -> u64 {
        self.0.values().sum()
    }

    pub fn merge(&mut self, other: &Exclusions) {
        for (k, v) in &other.0 {
            *self.0.entry(*k).or_insert(0) += v;
        }
    }
}

/// Net movement of one mint for one owner, kept as a raw integer until the
/// very end so decimals never round twice.
struct MintDelta {
    raw: i128,
    decimals: u8,
}

/// Infers at most one trade from a transaction.
pub fn infer_trade(
    tx: &RpcTransaction,
    slot: u64,
    tx_index: usize,
    block_time: Option<i64>,
) -> Result<Trade, Excluded> {
    let Some(meta) = tx.meta.as_ref() else {
        return Err(Excluded::Failed);
    };
    if meta.err.as_ref().is_some_and(|e| !e.is_null()) {
        return Err(Excluded::Failed);
    }
    let Some(block_time) = block_time else {
        return Err(Excluded::NoBlockTime);
    };

    // The fee payer is the first account key, and is the party whose balance
    // changes we read as "the trade". Aggregating across all owners would net
    // to zero, since one side's loss is the other side's gain.
    let Some(trader) = tx.transaction.message.account_keys.first() else {
        return Err(Excluded::NoTokenMovement);
    };

    // Net token movement per mint for the trader, keyed by token account index
    // so accounts created inside the transaction start from zero.
    let mut pre_amounts: HashMap<usize, i128> = HashMap::new();
    let mut pre_owned: HashSet<usize> = HashSet::new();
    for b in &meta.pre_token_balances {
        pre_amounts.insert(b.account_index, b.ui_token_amount.raw());
        if b.owner.as_deref() == Some(trader.as_str()) {
            pre_owned.insert(b.account_index);
        }
    }

    let mut by_mint: HashMap<&str, MintDelta> = HashMap::new();
    let mut post_owned: HashSet<usize> = HashSet::new();

    for b in &meta.post_token_balances {
        if b.owner.as_deref() != Some(trader.as_str()) {
            continue;
        }
        post_owned.insert(b.account_index);
        let delta =
            b.ui_token_amount.raw() - pre_amounts.get(&b.account_index).copied().unwrap_or(0);
        let e = by_mint.entry(b.mint.as_str()).or_insert(MintDelta {
            raw: 0,
            decimals: b.ui_token_amount.decimals,
        });
        e.raw += delta;
    }
    // Token accounts that existed before but were closed during the transaction.
    for b in &meta.pre_token_balances {
        if post_owned.contains(&b.account_index) || b.owner.as_deref() != Some(trader.as_str()) {
            continue;
        }
        let e = by_mint.entry(b.mint.as_str()).or_insert(MintDelta {
            raw: 0,
            decimals: b.ui_token_amount.decimals,
        });
        e.raw -= b.ui_token_amount.raw();
    }

    // Native SOL movement, with the fee added back so it is not mistaken for
    // part of the trade.
    let mut native_delta = match (meta.pre_balances.first(), meta.post_balances.first()) {
        (Some(&p), Some(&q)) => q as i128 - p as i128 + meta.fee as i128,
        _ => 0,
    };

    // Opening a token account locks up rent and closing one refunds it. That
    // movement is not part of the trade, and left in it prices a plain airdrop
    // into a fresh account as if the rent had bought the tokens. Both sides are
    // removed so only the exchanged value remains.
    let created = post_owned.difference(&pre_owned).count() as i128;
    let closed = pre_owned.difference(&post_owned).count() as i128;
    native_delta += (created - closed) * TOKEN_ACCOUNT_RENT_LAMPORTS;
    // Wrapped SOL is the same asset; wrapping cancels out against the native leg.
    let wsol_delta = by_mint.get(WSOL_MINT).map(|d| d.raw).unwrap_or(0);
    let sol_lamports = native_delta + wsol_delta;

    let mut movers = by_mint
        .iter()
        .filter(|(m, d)| **m != WSOL_MINT && d.raw != 0);
    let Some((mint, delta)) = movers.next() else {
        return Err(if sol_lamports != 0 {
            Excluded::WrapOrTransferOnly
        } else {
            Excluded::NoTokenMovement
        });
    };
    if movers.next().is_some() {
        return Err(Excluded::MultiHop);
    }

    if sol_lamports == 0 {
        return Err(Excluded::NoSolLeg);
    }
    // Opposite signs mean one asset was exchanged for the other. Same signs
    // mean both entered or both left the wallet, which is a liquidity action.
    if (sol_lamports > 0) == (delta.raw > 0) {
        return Err(Excluded::SameDirection);
    }

    let token_amount = (delta.raw.abs() as f64) / 10f64.powi(delta.decimals as i32);
    let sol_amount = (sol_lamports.abs() as f64) / LAMPORTS_PER_SOL;
    if sol_amount < DUST_SOL {
        return Err(Excluded::Dust);
    }
    if token_amount <= 0.0 {
        return Err(Excluded::NoTokenMovement);
    }

    let price_sol = sol_amount / token_amount;
    if !price_sol.is_finite() || price_sol <= 0.0 {
        return Err(Excluded::Dust);
    }

    Ok(Trade {
        signature: tx
            .transaction
            .signatures
            .first()
            .cloned()
            .unwrap_or_default(),
        mint: (*mint).to_string(),
        slot,
        tx_index,
        block_time,
        token_amount,
        sol_amount,
        price_sol,
    })
}

pub fn infer_block_trades(slot: u64, block: &RpcBlock) -> (Vec<Trade>, Exclusions) {
    let mut trades = Vec::new();
    let mut excl = Exclusions::default();

    for (i, tx) in block.transactions.iter().enumerate() {
        match infer_trade(tx, slot, i, block.block_time) {
            Ok(t) => trades.push(t),
            Err(reason) => excl.bump(reason),
        }
    }
    (trades, excl)
}

// ---------------------------------------------------------------------------
// Candles
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct Candle {
    pub mint: String,
    pub interval_s: i64,
    pub bucket_ts: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    /// Volume is the SOL that changed hands: the quote-side notional, which is
    /// comparable across tokens with different decimals and supplies.
    pub volume_sol: f64,
    pub volume_token: f64,
    pub trade_count: u32,
}

pub fn bucket_start(block_time: i64, interval_s: i64) -> i64 {
    block_time - block_time.rem_euclid(interval_s)
}

/// Aggregates trades into candles. Trades are ordered by (slot, tx_index)
/// within a bucket so open and close reflect on-chain order rather than
/// arrival order. Empty buckets are omitted; gaps are not forward-filled.
pub fn build_candles(trades: &[Trade], interval_s: i64) -> Vec<Candle> {
    let mut ordered: Vec<&Trade> = trades.iter().collect();
    ordered.sort_by(|a, b| {
        a.mint
            .cmp(&b.mint)
            .then_with(|| a.slot.cmp(&b.slot))
            .then_with(|| a.tx_index.cmp(&b.tx_index))
    });

    let mut out: Vec<Candle> = Vec::new();
    let mut current: Option<Candle> = None;

    for t in ordered {
        let bucket = bucket_start(t.block_time, interval_s);
        let same = current
            .as_ref()
            .is_some_and(|c| c.mint == t.mint && c.bucket_ts == bucket);

        if !same {
            if let Some(c) = current.take() {
                out.push(c);
            }
            current = Some(Candle {
                mint: t.mint.clone(),
                interval_s,
                bucket_ts: bucket,
                open: t.price_sol,
                high: t.price_sol,
                low: t.price_sol,
                close: t.price_sol,
                volume_sol: 0.0,
                volume_token: 0.0,
                trade_count: 0,
            });
        }

        let c = current.as_mut().expect("candle initialised above");
        c.high = c.high.max(t.price_sol);
        c.low = c.low.min(t.price_sol);
        c.close = t.price_sol;
        c.volume_sol += t.sol_amount;
        c.volume_token += t.token_amount;
        c.trade_count += 1;
    }

    if let Some(c) = current {
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const MINT: &str = "MintAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const TRADER: &str = "TraderAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    const POOL: &str = "PoolAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    struct TxBuilder {
        keys: Vec<String>,
        fee: u64,
        err: serde_json::Value,
        pre_sol: Vec<u64>,
        post_sol: Vec<u64>,
        pre_tok: Vec<serde_json::Value>,
        post_tok: Vec<serde_json::Value>,
    }

    impl TxBuilder {
        fn new() -> Self {
            Self {
                keys: vec![TRADER.into(), POOL.into()],
                fee: 5000,
                err: json!(null),
                pre_sol: vec![10_000_000_000, 0],
                post_sol: vec![9_999_995_000, 0],
                pre_tok: vec![],
                post_tok: vec![],
            }
        }

        /// Fee payer's native balance change, before the fee is deducted.
        fn sol(mut self, lamports: i64) -> Self {
            self.post_sol[0] = (self.pre_sol[0] as i64 + lamports - self.fee as i64) as u64;
            self
        }

        fn token(mut self, mint: &str, owner: &str, decimals: u8, pre: i128, post: i128) -> Self {
            let idx = self.pre_tok.len() + self.post_tok.len() + 2;
            let entry = |amount: i128| {
                json!({
                    "accountIndex": idx,
                    "mint": mint,
                    "owner": owner,
                    "uiTokenAmount": {"amount": amount.to_string(), "decimals": decimals},
                })
            };
            self.pre_tok.push(entry(pre));
            self.post_tok.push(entry(post));
            self
        }

        /// A token account that did not exist before the transaction.
        fn token_new(mut self, mint: &str, owner: &str, decimals: u8, post: i128) -> Self {
            let idx = self.pre_tok.len() + self.post_tok.len() + 2;
            self.post_tok.push(json!({
                "accountIndex": idx,
                "mint": mint,
                "owner": owner,
                "uiTokenAmount": {"amount": post.to_string(), "decimals": decimals},
            }));
            self
        }

        /// A token account that existed before and was closed during the transaction.
        fn token_closed(mut self, mint: &str, owner: &str, decimals: u8, pre: i128) -> Self {
            let idx = self.pre_tok.len() + self.post_tok.len() + 2;
            self.pre_tok.push(json!({
                "accountIndex": idx,
                "mint": mint,
                "owner": owner,
                "uiTokenAmount": {"amount": pre.to_string(), "decimals": decimals},
            }));
            self
        }

        fn build(self) -> RpcTransaction {
            serde_json::from_value(json!({
                "version": 0,
                "transaction": {
                    "signatures": ["sig1"],
                    "message": {
                        "accountKeys": self.keys,
                        "header": {
                            "numRequiredSignatures": 1,
                            "numReadonlySignedAccounts": 0,
                            "numReadonlyUnsignedAccounts": 0,
                        },
                        "instructions": [],
                    },
                },
                "meta": {
                    "err": self.err,
                    "fee": self.fee,
                    "preBalances": self.pre_sol,
                    "postBalances": self.post_sol,
                    "preTokenBalances": self.pre_tok,
                    "postTokenBalances": self.post_tok,
                },
            }))
            .expect("fixture should deserialize")
        }
    }

    fn infer(tx: &RpcTransaction) -> Result<Trade, Excluded> {
        infer_trade(tx, 100, 0, Some(1_700_000_000))
    }

    #[test]
    fn prices_a_buy_from_balance_changes() {
        // Spend 2 SOL, receive 1000 tokens with 6 decimals -> 0.002 SOL each.
        let tx = TxBuilder::new()
            .sol(-2_000_000_000)
            .token(MINT, TRADER, 6, 0, 1_000_000_000)
            .build();

        let t = infer(&tx).expect("should be a trade");
        assert_eq!(t.mint, MINT);
        assert!((t.sol_amount - 2.0).abs() < 1e-9, "sol {}", t.sol_amount);
        assert!(
            (t.token_amount - 1000.0).abs() < 1e-9,
            "tokens {}",
            t.token_amount
        );
        assert!((t.price_sol - 0.002).abs() < 1e-12, "price {}", t.price_sol);
    }

    #[test]
    fn prices_a_sell_from_balance_changes() {
        // Sell 1000 tokens for 3 SOL.
        let tx = TxBuilder::new()
            .sol(3_000_000_000)
            .token(MINT, TRADER, 6, 1_000_000_000, 0)
            .build();

        let t = infer(&tx).expect("should be a trade");
        assert!((t.price_sol - 0.003).abs() < 1e-12, "price {}", t.price_sol);
        assert!((t.token_amount - 1000.0).abs() < 1e-9);
    }

    #[test]
    fn applies_token_decimals() {
        // Same raw amount, 9 decimals instead of 6: price differs 1000x.
        let tx = TxBuilder::new()
            .sol(-2_000_000_000)
            .token(MINT, TRADER, 9, 0, 1_000_000_000)
            .build();

        let t = infer(&tx).unwrap();
        assert!(
            (t.token_amount - 1.0).abs() < 1e-9,
            "tokens {}",
            t.token_amount
        );
        assert!((t.price_sol - 2.0).abs() < 1e-9, "price {}", t.price_sol);
    }

    #[test]
    fn the_fee_is_not_counted_as_trade_volume() {
        // A trade of exactly 2 SOL, plus a fee. sol() already excludes the fee,
        // so the inferred amount must be the round 2 SOL.
        let tx = TxBuilder::new()
            .sol(-2_000_000_000)
            .token(MINT, TRADER, 6, 0, 1_000_000_000)
            .build();
        assert!((infer(&tx).unwrap().sol_amount - 2.0).abs() < 1e-9);
    }

    #[test]
    fn ignores_balance_changes_of_other_owners() {
        // The pool's side of the trade must not be read as a second mint.
        let tx = TxBuilder::new()
            .sol(-2_000_000_000)
            .token(MINT, TRADER, 6, 0, 1_000_000_000)
            .token(MINT, POOL, 6, 5_000_000_000, 4_000_000_000)
            .build();

        let t = infer(&tx).expect("pool leg should be ignored");
        assert!((t.token_amount - 1000.0).abs() < 1e-9);
    }

    #[test]
    fn excludes_failed_transactions() {
        let mut b = TxBuilder::new()
            .sol(-2_000_000_000)
            .token(MINT, TRADER, 6, 0, 1_000_000_000);
        b.err = json!({"InstructionError": [0, "Custom"]});
        assert_eq!(infer(&b.build()), Err(Excluded::Failed));
    }

    #[test]
    fn excludes_multi_hop_routes() {
        const MINT2: &str = "MintBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";
        let tx = TxBuilder::new()
            .sol(-2_000_000_000)
            .token(MINT, TRADER, 6, 0, 1_000_000_000)
            .token(MINT2, TRADER, 6, 0, 500_000_000)
            .build();
        assert_eq!(infer(&tx), Err(Excluded::MultiHop));
    }

    #[test]
    fn excludes_plain_wrapping() {
        // Native SOL down, wrapped SOL up by the same amount: no trade.
        let tx = TxBuilder::new()
            .sol(-1_000_000_000)
            .token(WSOL_MINT, TRADER, 9, 0, 1_000_000_000)
            .build();
        assert_eq!(infer(&tx), Err(Excluded::NoTokenMovement));
    }

    #[test]
    fn excludes_a_token_transfer_with_no_sol_leg() {
        let tx = TxBuilder::new()
            .token(MINT, TRADER, 6, 1_000_000_000, 0)
            .build();
        assert_eq!(infer(&tx), Err(Excluded::NoSolLeg));
    }

    #[test]
    fn excludes_liquidity_provision() {
        // Both the token and SOL leave the wallet: an LP deposit, not a trade.
        let tx = TxBuilder::new()
            .sol(-2_000_000_000)
            .token(MINT, TRADER, 6, 1_000_000_000, 0)
            .build();
        assert_eq!(infer(&tx), Err(Excluded::SameDirection));
    }

    #[test]
    fn excludes_dust_trades() {
        let tx = TxBuilder::new()
            .sol(-100_000) // 0.0001 SOL, below the threshold
            .token(MINT, TRADER, 6, 0, 1_000)
            .build();
        assert_eq!(infer(&tx), Err(Excluded::Dust));
    }

    #[test]
    fn excludes_a_pure_sol_transfer() {
        let tx = TxBuilder::new().sol(-1_000_000_000).build();
        assert_eq!(infer(&tx), Err(Excluded::WrapOrTransferOnly));
    }

    #[test]
    fn handles_a_swap_routed_through_wrapped_sol() {
        // The wallet wraps 2 SOL and spends all of it on tokens: the native and
        // WSOL legs combine into a single 2 SOL outflow.
        let tx = TxBuilder::new()
            .sol(-2_000_000_000)
            .token(WSOL_MINT, TRADER, 9, 0, 0)
            .token(MINT, TRADER, 6, 0, 1_000_000_000)
            .build();

        let t = infer(&tx).expect("should price through the WSOL leg");
        assert!((t.sol_amount - 2.0).abs() < 1e-9);
        assert!((t.price_sol - 0.002).abs() < 1e-12);
    }

    #[test]
    fn account_rent_is_not_mistaken_for_a_purchase() {
        // A token lands in a freshly opened account. The only SOL that moved is
        // the rent for that account, so once rent is removed nothing is left to
        // price: this is a transfer, not a trade. Before the rent adjustment
        // this priced 150 tokens at the cost of the rent.
        let tx = TxBuilder::new()
            .sol(-(TOKEN_ACCOUNT_RENT_LAMPORTS as i64))
            .token_new(MINT, TRADER, 6, 150_000_000)
            .build();

        assert_eq!(infer(&tx), Err(Excluded::NoSolLeg));
    }

    #[test]
    fn rent_is_removed_from_a_genuine_trade() {
        // A real 2 SOL buy that also opens the token account: the rent must not
        // inflate the price.
        let tx = TxBuilder::new()
            .sol(-2_000_000_000 - TOKEN_ACCOUNT_RENT_LAMPORTS as i64)
            .token_new(MINT, TRADER, 6, 1_000_000_000)
            .build();

        let t = infer(&tx).expect("should still be a trade");
        assert!(
            (t.sol_amount - 2.0).abs() < 1e-9,
            "rent leaked into volume: {}",
            t.sol_amount
        );
        assert!((t.price_sol - 0.002).abs() < 1e-12);
    }

    #[test]
    fn closing_an_account_refunds_rent_without_becoming_a_sale() {
        // Selling the whole balance and closing the account refunds rent; the
        // refund must not be counted as trade proceeds.
        let tx = TxBuilder::new()
            .sol(3_000_000_000 + TOKEN_ACCOUNT_RENT_LAMPORTS as i64)
            .token_closed(MINT, TRADER, 6, 1_000_000_000)
            .build();

        let t = infer(&tx).expect("should be a sale");
        assert!(
            (t.sol_amount - 3.0).abs() < 1e-9,
            "rent refund leaked in: {}",
            t.sol_amount
        );
    }

    // ----------------------------- candles ---------------------------------

    fn trade(ts: i64, slot: u64, idx: usize, price: f64, sol: f64) -> Trade {
        Trade {
            signature: format!("s{slot}-{idx}"),
            mint: MINT.into(),
            slot,
            tx_index: idx,
            block_time: ts,
            token_amount: sol / price,
            sol_amount: sol,
            price_sol: price,
        }
    }

    #[test]
    fn a_candle_takes_open_and_close_from_chain_order() {
        // Deliberately out of order in the input slice.
        let trades = vec![
            trade(BASE + 30, 12, 0, 3.0, 1.0),
            trade(BASE + 10, 10, 5, 1.0, 1.0),
            trade(BASE + 20, 10, 9, 2.0, 1.0),
        ];

        let candles = build_candles(&trades, INTERVAL_1M);
        assert_eq!(candles.len(), 1);
        let c = &candles[0];
        assert_eq!(c.open, 1.0, "earliest by (slot, tx_index)");
        assert_eq!(c.close, 3.0, "latest by (slot, tx_index)");
        assert_eq!(c.high, 3.0);
        assert_eq!(c.low, 1.0);
        assert_eq!(c.trade_count, 3);
        assert!((c.volume_sol - 3.0).abs() < 1e-9);
    }

    /// A timestamp divisible by both 60 and 300, so bucket edges are exact.
    const BASE: i64 = 1_699_999_800;

    #[test]
    fn buckets_align_to_the_interval() {
        assert_eq!(bucket_start(BASE + 59, INTERVAL_1M), BASE);
        assert_eq!(bucket_start(BASE + 60, INTERVAL_1M), BASE + 60);
        assert_eq!(bucket_start(BASE + 299, INTERVAL_5M), BASE);
        assert_eq!(bucket_start(BASE + 300, INTERVAL_5M), BASE + 300);
    }

    #[test]
    fn trades_split_across_minute_boundaries() {
        let trades = vec![
            trade(BASE + 10, 10, 0, 1.0, 1.0),
            trade(BASE + 70, 11, 0, 2.0, 1.0),
        ];
        let candles = build_candles(&trades, INTERVAL_1M);
        assert_eq!(candles.len(), 2);
        assert_eq!(candles[0].bucket_ts, BASE);
        assert_eq!(candles[1].bucket_ts, BASE + 60);
    }

    #[test]
    fn five_minute_volume_equals_the_sum_of_its_minutes() {
        let trades: Vec<Trade> = (0..5)
            .map(|i| trade(BASE + i * 60 + 5, 10 + i as u64, 0, 1.0 + i as f64, 2.0))
            .collect();

        let m1 = build_candles(&trades, INTERVAL_1M);
        let m5 = build_candles(&trades, INTERVAL_5M);

        assert_eq!(m1.len(), 5);
        assert_eq!(m5.len(), 1, "all five minutes fall in one 5m bucket");

        let sum: f64 = m1.iter().map(|c| c.volume_sol).sum();
        assert!((m5[0].volume_sol - sum).abs() < 1e-9);
        assert_eq!(m5[0].open, m1.first().unwrap().open);
        assert_eq!(m5[0].close, m1.last().unwrap().close);
        assert_eq!(
            m5[0].trade_count,
            m1.iter().map(|c| c.trade_count).sum::<u32>()
        );
    }

    #[test]
    fn candles_are_kept_separate_per_mint() {
        const MINT2: &str = "MintBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB";
        let mut a = trade(1_700_000_010, 10, 0, 1.0, 1.0);
        let mut b = trade(1_700_000_020, 10, 1, 9.0, 1.0);
        a.mint = MINT.into();
        b.mint = MINT2.into();

        let candles = build_candles(&[a, b], INTERVAL_1M);
        assert_eq!(candles.len(), 2);
        assert_ne!(candles[0].mint, candles[1].mint);
    }

    #[test]
    fn empty_buckets_are_omitted_not_filled() {
        let trades = vec![
            trade(BASE + 10, 10, 0, 1.0, 1.0),
            trade(BASE + 310, 20, 0, 2.0, 1.0), // five minutes later
        ];
        let candles = build_candles(&trades, INTERVAL_1M);
        assert_eq!(candles.len(), 2, "the gap produces no placeholder candles");
    }
}
