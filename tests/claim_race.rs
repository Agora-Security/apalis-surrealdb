#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;

use apalis_core::backend::TaskSink;
use apalis_surrealdb::{Config, Surreal, SurrealStorage, connect, queries::fetch_next::fetch_next};
use surrealdb::engine::any::Any;

const QUEUE: &str = "claim-race";
const ROUNDS: usize = 25;

async fn database() -> Arc<Surreal<Any>> {
    let conn = Arc::new(connect("mem://").await.unwrap());
    conn.use_ns("apalis").use_db("apalis").await.unwrap();
    SurrealStorage::setup(&conn).await.unwrap();
    conn
}

async fn run(conn: &Surreal<Any>, statement: &'static str) {
    conn.query(statement)
        .bind(("queue", QUEUE))
        .await
        .unwrap()
        .check()
        .unwrap();
}

async fn lock_owners(conn: &Surreal<Any>) -> Vec<Option<String>> {
    conn.query("SELECT VALUE lock_by FROM job WHERE queue = $queue AND status = 'Queued'")
        .bind(("queue", QUEUE))
        .await
        .unwrap()
        .take(0)
        .unwrap()
}

/// Two claimers race for one due row, as the eager poller and the live wake-up of one worker
/// did: one must win it, and the loser must see an empty batch rather than an error. Fails if a
/// lost commit surfaces as an error again, or if both claimers end up holding the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_claims_on_one_row_have_one_winner_and_no_error() {
    let conn = database().await;
    let config = Config::new(QUEUE);
    let mut storage = SurrealStorage::<u32, (), ()>::new_with_config(&conn, &config);

    for round in 0..ROUNDS {
        storage.push(u32::try_from(round).unwrap()).await.unwrap();
        // `run_at` is stored to the second, and the embedded engine's index misses a row due
        // within the current second; backdate it so the race starts on a claimable row. Both
        // updates go by id: one that rewrites an indexed field while scanning that index never
        // ends on the embedded engine.
        run(
            &conn,
            "UPDATE (SELECT VALUE id FROM job WHERE queue = $queue AND status = 'Pending') \
             SET run_at = time::now() - 1m",
        )
        .await;

        let (first_conn, second_conn) = (conn.clone(), conn.clone());
        let (first_config, second_config) = (config.clone(), config.clone());
        let first =
            tokio::spawn(async move { fetch_next(&first_conn, &first_config, "worker-a").await });
        let second =
            tokio::spawn(async move { fetch_next(&second_conn, &second_config, "worker-b").await });

        let first_claimed = first.await.unwrap().expect("first claimer errored");
        let second_claimed = second.await.unwrap().expect("second claimer errored");
        assert_eq!(
            first_claimed.len() + second_claimed.len(),
            1,
            "round {round}: exactly one claimer must win the row"
        );

        let winner = if first_claimed.is_empty() {
            "worker-b"
        } else {
            "worker-a"
        };
        assert_eq!(lock_owners(&conn).await, vec![Some(winner.to_owned())]);

        // retire the claimed row so the next round races for a fresh one
        run(
            &conn,
            "UPDATE (SELECT VALUE id FROM job WHERE queue = $queue AND status = 'Queued') \
             SET status = 'Done', done_at = time::now()",
        )
        .await;
    }
}
