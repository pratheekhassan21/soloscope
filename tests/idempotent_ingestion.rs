//! Re-ingesting the same block must not duplicate or corrupt stored data.
//! This drives the same analysis and write path the pipeline uses.

use rusqlite::Connection;
use solscope::db::{self, BlockWrite};
use solscope::ingest::analyse;
use solscope::model::RpcBlock;

const FIXTURE_SLOT: u64 = 439_583_562;

fn fixture() -> RpcBlock {
    serde_json::from_str(include_str!("fixtures/block.json")).expect("fixture should parse")
}

fn apply(conn: &mut Connection, write: &BlockWrite) {
    let tx = conn.transaction().unwrap();
    db::write_block(&tx, write).unwrap();
    tx.commit().unwrap();
}

/// Row counts and aggregate values that must be identical between runs.
fn snapshot(conn: &Connection) -> Vec<(String, i64, f64)> {
    let count = |sql: &str| conn.query_row(sql, [], |r| r.get::<_, i64>(0)).unwrap();
    let sum = |sql: &str| {
        conn.query_row(sql, [], |r| r.get::<_, Option<f64>>(0))
            .unwrap()
            .unwrap_or(0.0)
    };

    vec![
        ("slots".into(), count("SELECT COUNT(*) FROM slots"), 0.0),
        (
            "transactions".into(),
            count("SELECT COUNT(*) FROM transactions"),
            0.0,
        ),
        (
            "conflicts".into(),
            count("SELECT COUNT(*) FROM account_conflicts"),
            0.0,
        ),
        (
            "contention".into(),
            count("SELECT COUNT(*) FROM contention"),
            0.0,
        ),
        (
            "trades".into(),
            count("SELECT COUNT(*) FROM trades"),
            sum("SELECT SUM(sol_amount) FROM trades"),
        ),
        (
            "candles".into(),
            count("SELECT COUNT(*) FROM candles"),
            sum("SELECT SUM(volume_sol) FROM candles"),
        ),
    ]
}

#[test]
fn ingesting_the_same_block_repeatedly_is_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("idem.db");
    let mut conn = db::open_writer(&path).unwrap();

    let block = fixture();
    let (write, _) = analyse(FIXTURE_SLOT, &block, true);

    apply(&mut conn, &write);
    db::rebuild_candles(&mut conn).unwrap();
    let first = snapshot(&conn);

    assert!(first[1].1 > 0, "fixture should produce transactions");

    // Three more passes over exactly the same input.
    for _ in 0..3 {
        let (write, _) = analyse(FIXTURE_SLOT, &block, true);
        apply(&mut conn, &write);
        db::rebuild_candles(&mut conn).unwrap();
    }

    assert_eq!(snapshot(&conn), first, "re-ingestion changed stored data");
}

#[test]
fn candle_volume_does_not_accumulate_across_runs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("vol.db");
    let mut conn = db::open_writer(&path).unwrap();

    let block = fixture();
    let (write, _) = analyse(FIXTURE_SLOT, &block, true);

    apply(&mut conn, &write);
    db::rebuild_candles(&mut conn).unwrap();
    let volume_after_one: f64 = conn
        .query_row(
            "SELECT COALESCE(SUM(volume_sol),0) FROM candles WHERE interval_s = 60",
            [],
            |r| r.get(0),
        )
        .unwrap();

    apply(&mut conn, &write);
    db::rebuild_candles(&mut conn).unwrap();
    let volume_after_two: f64 = conn
        .query_row(
            "SELECT COALESCE(SUM(volume_sol),0) FROM candles WHERE interval_s = 60",
            [],
            |r| r.get(0),
        )
        .unwrap();

    assert!(
        (volume_after_one - volume_after_two).abs() < 1e-9,
        "volume doubled on re-ingest: {volume_after_one} then {volume_after_two}"
    );
}

#[test]
fn a_completed_slot_is_not_refetched() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("resume.db");
    let mut conn = db::open_writer(&path).unwrap();

    let block = fixture();
    let (write, _) = analyse(FIXTURE_SLOT, &block, true);
    apply(&mut conn, &write);

    let done = db::completed_slots(&conn, FIXTURE_SLOT, FIXTURE_SLOT + 1).unwrap();
    assert_eq!(
        done,
        vec![FIXTURE_SLOT],
        "a finished slot should be skipped on re-run"
    );

    let none = db::completed_slots(&conn, FIXTURE_SLOT + 1, FIXTURE_SLOT + 10).unwrap();
    assert!(none.is_empty(), "unfetched slots must still be pending");
}

#[test]
fn reingesting_a_slot_replaces_rather_than_appends() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("replace.db");
    let mut conn = db::open_writer(&path).unwrap();

    let block = fixture();
    let (full, _) = analyse(FIXTURE_SLOT, &block, true);
    apply(&mut conn, &full);

    let before: i64 = conn
        .query_row("SELECT COUNT(*) FROM transactions", [], |r| r.get(0))
        .unwrap();
    assert!(before > 0);

    // The same slot re-observed as empty: stale rows must go.
    let empty = BlockWrite::skipped(FIXTURE_SLOT);
    apply(&mut conn, &empty);

    let after: i64 = conn
        .query_row("SELECT COUNT(*) FROM transactions", [], |r| r.get(0))
        .unwrap();
    assert_eq!(after, 0, "stale transactions survived a re-ingest");

    let slots: i64 = conn
        .query_row("SELECT COUNT(*) FROM slots", [], |r| r.get(0))
        .unwrap();
    assert_eq!(slots, 1, "the slot row should be updated, not duplicated");
}
