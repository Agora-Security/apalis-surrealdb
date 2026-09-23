use std::future::IntoFuture;

use surrealdb::{Surreal, engine::any::Any};

use crate::{
    CompactType, SurrealError, SurrealTask,
    from_row::SurrealTaskRow,
    queries::{
        MAX_TX_RETRIES, TxOutcome, classify_tx_errors, fetch_next::decode_row,
        is_retryable_transport, kill_task::kill_task, timeout_error, tx_conflict_backoff,
        tx_unavailable_backoff, with_timeout,
    },
};

const FETCH_NEXT_SHARED: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/fetch_next_shared.surql"
));

/// Atomically claim the next batch of due tasks across several queues for the shared driver
pub async fn fetch_next_shared(
    conn: &Surreal<Any>,
    queues: &[String],
    limit: i64,
) -> Result<Vec<SurrealTask<CompactType>>, SurrealError> {
    let mut last_err = None;
    for attempt in 0..MAX_TX_RETRIES {
        let queried = with_timeout(
            "fetch_next_shared",
            conn.query(FETCH_NEXT_SHARED)
                .bind(("queues", queues.to_vec()))
                .bind(("limit", limit))
                .into_future(),
        )
        .await;
        // resending on a connection that stopped answering cannot help; the fetcher backs off
        let Some(queried) = queried else {
            return Err(SurrealError::Database(timeout_error("fetch_next_shared")));
        };
        let mut response = match queried {
            Ok(response) => response,
            Err(err) if is_retryable_transport(&err) => {
                last_err = Some(err);
                tx_unavailable_backoff("fetch_next_shared", attempt).await;
                continue;
            }
            Err(err) => return Err(SurrealError::Database(err)),
        };
        match classify_tx_errors(response.take_errors()) {
            Some(TxOutcome::Retry(err)) => {
                last_err = Some(err);
                tx_conflict_backoff("fetch_next_shared", attempt).await;
                continue;
            }
            Some(TxOutcome::Unavailable(err)) => {
                last_err = Some(err);
                tx_unavailable_backoff("fetch_next_shared", attempt).await;
                continue;
            }
            Some(TxOutcome::Fail(err)) => return Err(SurrealError::Database(err)),
            None => {}
        }
        let rows: Vec<SurrealTaskRow> = response.take(5)?;
        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let id = row.id.clone();
            match decode_row(row) {
                Ok(task) => tasks.push(task),
                Err(e) => {
                    log::error!("Killing task {id:?} the worker cannot decode: {e}");
                    if let Err(e) = kill_task(conn, &id, &e.to_string()).await {
                        log::error!("Failed to kill undecodable task {id:?}: {e}");
                    }
                }
            }
        }
        return Ok(tasks);
    }
    // surface the failure after exhausting retries instead of faking an empty batch
    Err(SurrealError::Database(last_err.unwrap_or_else(|| {
        surrealdb::Error::query(
            "Transaction conflict retries exhausted".to_owned(),
            surrealdb::types::QueryError::TransactionConflict,
        )
    })))
}
