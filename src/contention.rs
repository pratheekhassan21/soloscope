
use std::collections::BTreeSet;
use std::collections::HashMap;

use crate::model::{ResolvedBlock, ResolvedTx};

/// Per-slot conflict attribution is capped so one pathological block cannot
/// dominate the table; accounts are kept in descending order of delays caused.
pub const MAX_CONFLICT_ROWS_PER_SLOT: usize = 25;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Schedule {
    /// Step assigned to each input transaction, 1-based and aligned to the input slice.
    pub steps: Vec<usize>,
    pub depth: usize,
    /// Transactions per step, indexed from step 1.
    pub widths: Vec<usize>,
}

impl Schedule {
    pub fn tx_count(&self) -> usize {
        self.steps.len()
    }

    /// Mean transactions per step: how much of the block could have run in parallel.
    pub fn parallelism(&self) -> f64 {
        if self.depth == 0 {
            0.0
        } else {
            self.steps.len() as f64 / self.depth as f64
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConflictStat {
    pub account: String,
    /// Transactions that took a write lock on this account.
    pub write_locks: u32,
    /// Transactions that took a read lock on this account.
    pub read_locks: u32,
    /// Times this account pushed a transaction into a later step.
    pub delays: u32,
    /// Programs invoked by the transactions this account delayed.
    pub programs: BTreeSet<String>,
}

#[derive(Debug, Clone)]
pub struct BlockAnalysis {
    /// Schedule over every transaction in the block.
    pub all: Schedule,
    /// Schedule with vote transactions removed. Votes are ~70% of a block and
    /// barely contend, so they inflate parallelism; both are reported.
    pub non_vote: Schedule,
    /// Accounts that caused delays, most contended first.
    pub conflicts: Vec<ConflictStat>,
}


#[derive(Debug, Clone, Copy)]
enum Blocker {
    /// An earlier write lock blocked this transaction.
    Write,
    /// Earlier read locks blocked this transaction's write.
    Read,
}

#[derive(Default)]
struct AccountState {
    /// Step of the most recent transaction to write-lock this account.
    last_writer: usize,
    /// Latest step among transactions that read-locked this account.
    max_reader: usize,
    write_locks: u32,
    read_locks: u32,
    delays: u32,
}

fn build(txs: &[&ResolvedTx], attribute: bool) -> (Schedule, Vec<ConflictStat>) {
    let mut state: HashMap<&str, AccountState> = HashMap::new();
    let mut programs_by_account: HashMap<&str, BTreeSet<String>> = HashMap::new();
    let mut steps = Vec::with_capacity(txs.len());
    let mut widths: Vec<usize> = Vec::new();

    for tx in txs {
        // Earliest step this transaction may occupy.
        let mut step = 1;
        for a in &tx.readonly {
            // A read only waits for the preceding write.
            if let Some(s) = state.get(a.as_str()) {
                step = step.max(s.last_writer + 1);
            }
        }
        for a in &tx.writable {
            // A write waits for preceding reads and writes alike.
            if let Some(s) = state.get(a.as_str()) {
                step = step.max(s.last_writer.max(s.max_reader) + 1);
            }
        }

        // Attribute the delay to whichever accounts are on the critical path.
        if attribute && step > 1 {
            let prev = step - 1;
            for a in &tx.readonly {
                if state.get(a.as_str()).is_some_and(|s| s.last_writer == prev) {
                    record_delay(&mut state, &mut programs_by_account, a, tx, Blocker::Write);
                }
            }
            for a in &tx.writable {
                match state.get(a.as_str()) {
                    Some(s) if s.last_writer == prev => {
                        record_delay(&mut state, &mut programs_by_account, a, tx, Blocker::Write)
                    }
                    Some(s) if s.max_reader == prev => {
                        record_delay(&mut state, &mut programs_by_account, a, tx, Blocker::Read)
                    }
                    _ => {}
                }
            }
        }

        // Publish this transaction's locks for the transactions that follow.
        for a in &tx.readonly {
            let s = state.entry(a.as_str()).or_default();
            s.max_reader = s.max_reader.max(step);
            s.read_locks += 1;
        }
        for a in &tx.writable {
            let s = state.entry(a.as_str()).or_default();
            s.last_writer = step;
            s.write_locks += 1;
        }

        if widths.len() < step {
            widths.resize(step, 0);
        }
        widths[step - 1] += 1;
        steps.push(step);
    }

    let schedule = Schedule {
        depth: widths.len(),
        steps,
        widths,
    };

    let mut conflicts: Vec<ConflictStat> = if attribute {
        state
            .iter()
            .filter(|(_, s)| s.delays > 0)
            .map(|(account, s)| ConflictStat {
                account: (*account).to_string(),
                write_locks: s.write_locks,
                read_locks: s.read_locks,
                delays: s.delays,
                programs: programs_by_account
                    .get(account)
                    .cloned()
                    .unwrap_or_default(),
            })
            .collect()
    } else {
        Vec::new()
    };

    conflicts.sort_by(|a, b| {
        b.delays
            .cmp(&a.delays)
            .then_with(|| (b.write_locks + b.read_locks).cmp(&(a.write_locks + a.read_locks)))
            .then_with(|| a.account.cmp(&b.account))
    });
    conflicts.truncate(MAX_CONFLICT_ROWS_PER_SLOT);

    (schedule, conflicts)
}

fn record_delay<'a>(
    state: &mut HashMap<&'a str, AccountState>,
    programs: &mut HashMap<&'a str, BTreeSet<String>>,
    account: &'a str,
    tx: &ResolvedTx,
    _kind: Blocker,
) {
    if let Some(s) = state.get_mut(account) {
        s.delays += 1;
    }
    let entry = programs.entry(account).or_default();
    for p in &tx.program_ids {
        entry.insert(p.clone());
    }
}

pub fn analyze(block: &ResolvedBlock) -> BlockAnalysis {
    let all_txs: Vec<&ResolvedTx> = block.transactions.iter().collect();
    let (all, conflicts) = build(&all_txs, true);

    let non_vote_txs: Vec<&ResolvedTx> = block.transactions.iter().filter(|t| !t.is_vote).collect();
    let (non_vote, _) = build(&non_vote_txs, false);

    BlockAnalysis {
        all,
        non_vote,
        conflicts,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx(index: usize, writable: &[&str], readonly: &[&str]) -> ResolvedTx {
        ResolvedTx {
            signature: format!("sig{index}"),
            index,
            succeeded: true,
            fee: 5000,
            is_vote: false,
            writable: writable.iter().map(|s| s.to_string()).collect(),
            readonly: readonly.iter().map(|s| s.to_string()).collect(),
            program_ids: vec!["prog".into()],
        }
    }

    fn schedule_of(txs: &[ResolvedTx]) -> Schedule {
        let refs: Vec<&ResolvedTx> = txs.iter().collect();
        build(&refs, true).0
    }

    #[test]
    fn disjoint_transactions_all_run_in_one_step() {
        let txs = vec![tx(0, &["a"], &[]), tx(1, &["b"], &[]), tx(2, &["c"], &[])];
        let s = schedule_of(&txs);
        assert_eq!(s.depth, 1);
        assert_eq!(s.widths, vec![3]);
        assert_eq!(s.parallelism(), 3.0);
    }

    #[test]
    fn chained_writers_serialise_completely() {
        let txs: Vec<ResolvedTx> = (0..5).map(|i| tx(i, &["hot"], &[])).collect();
        let s = schedule_of(&txs);
        assert_eq!(s.depth, 5, "each write must wait for the previous one");
        assert_eq!(s.steps, vec![1, 2, 3, 4, 5]);
        assert_eq!(s.widths, vec![1, 1, 1, 1, 1]);
    }

    #[test]
    fn concurrent_reads_share_a_step() {
        let txs: Vec<ResolvedTx> = (0..4).map(|i| tx(i, &[], &["shared"])).collect();
        let s = schedule_of(&txs);
        assert_eq!(s.depth, 1, "read/read does not conflict");
        assert_eq!(s.widths, vec![4]);
    }

    #[test]
    fn a_write_conflicts_with_earlier_reads() {
        // Three readers share step 1; the writer must follow them.
        let txs = [
            tx(0, &[], &["shared"]),
            tx(1, &[], &["shared"]),
            tx(2, &[], &["shared"]),
            tx(3, &["shared"], &[]),
        ];
        let s = schedule_of(&txs);
        assert_eq!(s.steps, vec![1, 1, 1, 2]);
        assert_eq!(s.depth, 2);
    }

    #[test]
    fn a_read_conflicts_with_an_earlier_write() {
        let txs = vec![tx(0, &["shared"], &[]), tx(1, &[], &["shared"])];
        let s = schedule_of(&txs);
        assert_eq!(
            s.steps,
            vec![1, 2],
            "a read must wait for the preceding write"
        );
    }

    #[test]
    fn independent_chains_run_side_by_side() {
        // Two independent 3-long write chains: depth 3, width 2 throughout.
        let txs = [
            tx(0, &["a"], &[]),
            tx(1, &["b"], &[]),
            tx(2, &["a"], &[]),
            tx(3, &["b"], &[]),
            tx(4, &["a"], &[]),
            tx(5, &["b"], &[]),
        ];
        let s = schedule_of(&txs);
        assert_eq!(s.depth, 3);
        assert_eq!(s.widths, vec![2, 2, 2]);
    }

    #[test]
    fn a_readonly_account_never_serialises_writers() {
        // Every transaction reads the same program account but writes its own.
        let txs: Vec<ResolvedTx> = (0..10)
            .map(|i| tx(i, &[&format!("acct{i}")], &["program"]))
            .collect();
        let txs: Vec<ResolvedTx> = txs
            .into_iter()
            .enumerate()
            .map(|(i, mut t)| {
                t.writable = vec![format!("acct{i}")];
                t
            })
            .collect();
        let s = schedule_of(&txs);
        assert_eq!(s.depth, 1, "a shared read-only account is not a conflict");
    }

    #[test]
    fn lookup_table_accounts_conflict_like_static_ones() {
        // The scheduler sees resolved keys, so an address that arrived via a
        // lookup table contends with the same address used statically.
        let mut a = tx(0, &["alt_account"], &[]);
        a.writable = vec!["alt_account".into()];
        let b = tx(1, &["alt_account"], &[]);
        let s = schedule_of(&[a, b]);
        assert_eq!(s.steps, vec![1, 2]);
    }

    #[test]
    fn attributes_delays_to_the_responsible_account() {
        let txs = [
            tx(0, &["hot"], &[]),
            tx(1, &["hot"], &[]),
            tx(2, &["hot"], &[]),
        ];
        let refs: Vec<&ResolvedTx> = txs.iter().collect();
        let (_, conflicts) = build(&refs, true);

        assert_eq!(conflicts.len(), 1);
        let c = &conflicts[0];
        assert_eq!(c.account, "hot");
        assert_eq!(
            c.delays, 2,
            "two transactions were pushed later by this account"
        );
        assert_eq!(c.write_locks, 3);
        assert_eq!(c.read_locks, 0);
        assert!(c.programs.contains("prog"));
    }

    #[test]
    fn separates_read_and_write_lock_counts() {
        let txs = [
            tx(0, &[], &["mixed"]),
            tx(1, &[], &["mixed"]),
            tx(2, &["mixed"], &[]),
        ];
        let refs: Vec<&ResolvedTx> = txs.iter().collect();
        let (_, conflicts) = build(&refs, true);

        let c = conflicts.iter().find(|c| c.account == "mixed").unwrap();
        assert_eq!(c.read_locks, 2);
        assert_eq!(c.write_locks, 1);
        assert_eq!(c.delays, 1, "the write was delayed by the two reads");
    }

    #[test]
    fn uncontended_accounts_are_not_reported() {
        let txs = [tx(0, &["a"], &[]), tx(1, &["b"], &[])];
        let refs: Vec<&ResolvedTx> = txs.iter().collect();
        let (_, conflicts) = build(&refs, true);
        assert!(conflicts.is_empty());
    }

    #[test]
    fn votes_are_excluded_from_the_non_vote_schedule() {
        let mut vote = tx(0, &["vote_account"], &[]);
        vote.is_vote = true;
        let block = ResolvedBlock {
            slot: 1,
            block_time: Some(0),
            transactions: vec![vote, tx(1, &["hot"], &[]), tx(2, &["hot"], &[])],
        };

        let a = analyze(&block);
        assert_eq!(a.all.tx_count(), 3);
        assert_eq!(a.non_vote.tx_count(), 2);
        assert_eq!(
            a.non_vote.depth, 2,
            "the two conflicting non-vote txs still serialise"
        );
    }

    #[test]
    fn an_empty_block_has_zero_depth() {
        let block = ResolvedBlock {
            slot: 1,
            block_time: Some(0),
            transactions: vec![],
        };
        let a = analyze(&block);
        assert_eq!(a.all.depth, 0);
        assert_eq!(a.all.parallelism(), 0.0);
        assert!(a.conflicts.is_empty());
    }

    #[test]
    fn every_step_assignment_is_conflict_free() {
        // Property check: no two transactions sharing a step may conflict.
        let txs = vec![
            tx(0, &["a", "b"], &["r"]),
            tx(1, &["c"], &["a"]),
            tx(2, &["b"], &["r"]),
            tx(3, &["r"], &[]),
            tx(4, &["a"], &["c"]),
        ];
        let s = schedule_of(&txs);

        for (i, ti) in txs.iter().enumerate() {
            for (j, tj) in txs.iter().enumerate().skip(i + 1) {
                if s.steps[i] != s.steps[j] {
                    continue;
                }
                let conflict = ti
                    .writable
                    .iter()
                    .any(|w| tj.writable.contains(w) || tj.readonly.contains(w))
                    || tj.writable.iter().any(|w| ti.readonly.contains(w));
                assert!(
                    !conflict,
                    "tx {i} and {j} share step {} but conflict",
                    s.steps[i]
                );
            }
        }
    }
}
