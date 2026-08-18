//! Account-key resolution for versioned (v0) transactions, checked against a
//! real mainnet block captured in `tests/fixtures/block.json`.

use solscope::model::{RpcBlock, VOTE_PROGRAM, combined_account_keys, static_key_is_writable};

const FIXTURE_SLOT: u64 = 439_583_562;

fn fixture() -> RpcBlock {
    serde_json::from_str(include_str!("fixtures/block.json")).expect("fixture should parse")
}

#[test]
fn the_fixture_contains_v0_transactions_using_lookup_tables() {
    let block = fixture();
    let with_lookups = block
        .transactions
        .iter()
        .filter(|t| {
            t.meta.as_ref().is_some_and(|m| {
                !m.loaded_addresses.writable.is_empty() || !m.loaded_addresses.readonly.is_empty()
            })
        })
        .count();

    assert!(
        with_lookups > 0,
        "fixture must exercise address lookup tables"
    );
}

#[test]
fn lookup_table_addresses_are_appended_to_the_static_keys() {
    let block = fixture();

    for tx in &block.transactions {
        let meta = tx.meta.as_ref().expect("fixture transactions have meta");
        let loaded = &meta.loaded_addresses;
        if loaded.writable.is_empty() && loaded.readonly.is_empty() {
            continue;
        }

        let msg = &tx.transaction.message;
        let keys = combined_account_keys(msg, loaded);

        // Order matters: instruction indices address static keys, then the
        // writable lookups, then the readonly ones.
        let statics = msg.account_keys.len();
        assert_eq!(
            keys.len(),
            statics + loaded.writable.len() + loaded.readonly.len()
        );
        assert_eq!(&keys[..statics], &msg.account_keys[..]);
        assert_eq!(
            &keys[statics..statics + loaded.writable.len()],
            &loaded.writable[..]
        );
        assert_eq!(
            &keys[statics + loaded.writable.len()..],
            &loaded.readonly[..]
        );
    }
}

#[test]
fn resolved_lock_sets_cover_every_account_exactly_once() {
    let block = fixture();
    let resolved = block.resolve(FIXTURE_SLOT);

    for (tx, r) in block.transactions.iter().zip(&resolved.transactions) {
        let meta = tx.meta.as_ref().unwrap();
        let keys = combined_account_keys(&tx.transaction.message, &meta.loaded_addresses);

        assert_eq!(
            r.writable.len() + r.readonly.len(),
            keys.len(),
            "every account key must land in exactly one lock set"
        );

        let mut combined: Vec<&String> = r.writable.iter().chain(r.readonly.iter()).collect();
        let mut expected: Vec<&String> = keys.iter().collect();
        combined.sort();
        expected.sort();
        assert_eq!(combined, expected);
    }
}

#[test]
fn lookup_table_addresses_keep_the_writability_the_rpc_reported() {
    let block = fixture();
    let resolved = block.resolve(FIXTURE_SLOT);

    for (tx, r) in block.transactions.iter().zip(&resolved.transactions) {
        let loaded = &tx.meta.as_ref().unwrap().loaded_addresses;

        for addr in &loaded.writable {
            assert!(
                r.writable.contains(addr),
                "loaded writable address lost its write lock"
            );
        }
        for addr in &loaded.readonly {
            assert!(
                r.readonly.contains(addr),
                "loaded readonly address lost its read lock"
            );
        }
    }
}

#[test]
fn static_keys_follow_the_message_header() {
    let block = fixture();
    let resolved = block.resolve(FIXTURE_SLOT);

    for (tx, r) in block.transactions.iter().zip(&resolved.transactions) {
        let msg = &tx.transaction.message;
        let n = msg.account_keys.len();

        for (i, key) in msg.account_keys.iter().enumerate() {
            if static_key_is_writable(i, n, &msg.header) {
                assert!(
                    r.writable.contains(key),
                    "static key {i} should be writable"
                );
            } else {
                assert!(
                    r.readonly.contains(key),
                    "static key {i} should be readonly"
                );
            }
        }

        // The fee payer always signs and always pays, so it is always writable.
        if let Some(payer) = msg.account_keys.first() {
            assert!(r.writable.contains(payer), "fee payer must be writable");
        }
    }
}

#[test]
fn vote_transactions_are_identified() {
    let block = fixture();
    let resolved = block.resolve(FIXTURE_SLOT);

    let votes: Vec<_> = resolved.transactions.iter().filter(|t| t.is_vote).collect();
    assert!(
        !votes.is_empty(),
        "fixture should contain vote transactions"
    );

    for v in votes {
        assert_eq!(v.program_ids, vec![VOTE_PROGRAM.to_string()]);
    }

    // A transaction touching other programs must not be classified as a vote.
    for t in resolved.transactions.iter().filter(|t| !t.is_vote) {
        assert!(t.program_ids.iter().any(|p| p != VOTE_PROGRAM) || t.program_ids.is_empty());
    }
}
