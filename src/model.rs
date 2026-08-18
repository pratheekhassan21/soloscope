
use serde::Deserialize;

pub const VOTE_PROGRAM: &str = "Vote111111111111111111111111111111111111111";
pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcBlock {
    pub blockhash: String,
    pub parent_slot: u64,
    pub block_time: Option<i64>,
    pub block_height: Option<u64>,
    #[serde(default)]
    pub transactions: Vec<RpcTransaction>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcTransaction {
    /// `0` for v0 transactions, `"legacy"` for legacy ones. Absent on some providers.
    #[serde(default)]
    pub version: Option<serde_json::Value>,
    pub transaction: RpcTransactionInner,
    pub meta: Option<TxMeta>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcTransactionInner {
    #[serde(default)]
    pub signatures: Vec<String>,
    pub message: RpcMessage,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcMessage {
    #[serde(default)]
    pub account_keys: Vec<String>,
    pub header: MessageHeader,
    #[serde(default)]
    pub instructions: Vec<CompiledInstruction>,
    #[serde(default)]
    pub address_table_lookups: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageHeader {
    pub num_required_signatures: usize,
    pub num_readonly_signed_accounts: usize,
    pub num_readonly_unsigned_accounts: usize,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CompiledInstruction {
    pub program_id_index: usize,
    #[serde(default)]
    pub accounts: Vec<usize>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TxMeta {
    #[serde(default)]
    pub err: Option<serde_json::Value>,
    #[serde(default)]
    pub fee: u64,
    #[serde(default)]
    pub pre_balances: Vec<u64>,
    #[serde(default)]
    pub post_balances: Vec<u64>,
    #[serde(default)]
    pub pre_token_balances: Vec<TokenBalance>,
    #[serde(default)]
    pub post_token_balances: Vec<TokenBalance>,
    #[serde(default)]
    pub loaded_addresses: LoadedAddresses,
    #[serde(default)]
    pub compute_units_consumed: Option<u64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct LoadedAddresses {
    #[serde(default)]
    pub writable: Vec<String>,
    #[serde(default)]
    pub readonly: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenBalance {
    pub account_index: usize,
    pub mint: String,
    #[serde(default)]
    pub owner: Option<String>,
    pub ui_token_amount: UiTokenAmount,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UiTokenAmount {
    /// Raw integer amount as a string; the only field safe from float rounding.
    pub amount: String,
    pub decimals: u8,
}

impl UiTokenAmount {
    pub fn raw(&self) -> i128 {
        self.amount.parse::<i128>().unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// Resolved domain types
// ---------------------------------------------------------------------------

/// A transaction with its full account list resolved (static keys plus the
/// address-lookup-table entries the RPC resolved for us) and split into the
/// read and write lock sets the scheduler reasons about.
#[derive(Debug, Clone)]
pub struct ResolvedTx {
    pub signature: String,
    pub index: usize,
    pub succeeded: bool,
    pub fee: u64,
    pub is_vote: bool,
    pub writable: Vec<String>,
    pub readonly: Vec<String>,
    pub program_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedBlock {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub transactions: Vec<ResolvedTx>,
}

/// Account keys of a transaction in canonical order: static keys first, then
/// the lookup-table writable set, then the lookup-table readonly set.
/// Instruction indices address this combined list.
pub fn combined_account_keys(msg: &RpcMessage, loaded: &LoadedAddresses) -> Vec<String> {
    let mut keys =
        Vec::with_capacity(msg.account_keys.len() + loaded.writable.len() + loaded.readonly.len());
    keys.extend_from_slice(&msg.account_keys);
    keys.extend_from_slice(&loaded.writable);
    keys.extend_from_slice(&loaded.readonly);
    keys
}

/// Whether the static account at `index` is write-locked, per the message header.
///
/// Signed accounts occupy `[0, num_required_signatures)` and the last
/// `num_readonly_signed_accounts` of them are read-only. The remaining static
/// accounts are unsigned, and the last `num_readonly_unsigned_accounts` of
/// *those* are read-only. Lookup-table addresses are classified by the RPC
/// instead and are not covered here.
pub fn static_key_is_writable(index: usize, static_len: usize, header: &MessageHeader) -> bool {
    let signed = header.num_required_signatures;
    if index < signed {
        index < signed.saturating_sub(header.num_readonly_signed_accounts)
    } else {
        index < static_len.saturating_sub(header.num_readonly_unsigned_accounts)
    }
}

impl RpcTransaction {
    pub fn resolve(&self, index: usize) -> ResolvedTx {
        let meta = self.meta.clone().unwrap_or_default();
        let msg = &self.transaction.message;
        let static_len = msg.account_keys.len();
        let all_keys = combined_account_keys(msg, &meta.loaded_addresses);

        let mut writable = Vec::new();
        let mut readonly = Vec::new();

        for (i, key) in msg.account_keys.iter().enumerate() {
            if static_key_is_writable(i, static_len, &msg.header) {
                writable.push(key.clone());
            } else {
                readonly.push(key.clone());
            }
        }
        writable.extend(meta.loaded_addresses.writable.iter().cloned());
        readonly.extend(meta.loaded_addresses.readonly.iter().cloned());

        let mut program_ids: Vec<String> = msg
            .instructions
            .iter()
            .filter_map(|ix| all_keys.get(ix.program_id_index).cloned())
            .collect();
        program_ids.sort();
        program_ids.dedup();

        // A vote transaction invokes the vote program and nothing else. These
        // dominate block transaction counts and barely contend, so contention
        // metrics are reported both with and without them.
        let is_vote = !program_ids.is_empty() && program_ids.iter().all(|p| p == VOTE_PROGRAM);

        ResolvedTx {
            signature: self
                .transaction
                .signatures
                .first()
                .cloned()
                .unwrap_or_default(),
            index,
            succeeded: meta.err.as_ref().is_none_or(|e| e.is_null()),
            fee: meta.fee,
            is_vote,
            writable,
            readonly,
            program_ids,
        }
    }
}

impl RpcBlock {
    pub fn resolve(&self, slot: u64) -> ResolvedBlock {
        ResolvedBlock {
            slot,
            block_time: self.block_time,
            transactions: self
                .transactions
                .iter()
                .enumerate()
                .map(|(i, tx)| tx.resolve(i))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header(sig: usize, ro_signed: usize, ro_unsigned: usize) -> MessageHeader {
        MessageHeader {
            num_required_signatures: sig,
            num_readonly_signed_accounts: ro_signed,
            num_readonly_unsigned_accounts: ro_unsigned,
        }
    }

    #[test]
    fn classifies_static_keys_by_header() {
        // 5 static keys: 1 signer (writable), 4 unsigned of which 2 are readonly.
        let h = header(1, 0, 2);
        assert!(static_key_is_writable(0, 5, &h), "fee payer is writable");
        assert!(static_key_is_writable(1, 5, &h));
        assert!(static_key_is_writable(2, 5, &h));
        assert!(!static_key_is_writable(3, 5, &h), "readonly unsigned");
        assert!(!static_key_is_writable(4, 5, &h), "readonly unsigned");
    }

    #[test]
    fn classifies_readonly_signers() {
        // 2 signers, the second of which is readonly; 2 unsigned, 1 readonly.
        let h = header(2, 1, 1);
        assert!(static_key_is_writable(0, 4, &h));
        assert!(!static_key_is_writable(1, 4, &h), "readonly signer");
        assert!(static_key_is_writable(2, 4, &h));
        assert!(!static_key_is_writable(3, 4, &h));
    }
}
