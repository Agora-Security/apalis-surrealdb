use std::future::IntoFuture;

use surrealdb::{
    Surreal,
    engine::any::Any,
    types::{QueryError, RecordId},
};

use crate::{
    SurrealError,
    queries::{
        MAX_TX_RETRIES, TxOutcome, classify_tx_errors, is_retryable_transport, timeout_backoff,
        timeout_error, tx_conflict_backoff, tx_unavailable_backoff, with_timeout,
    },
};

const KILL_TASK: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/task/kill_task.surql"
));

pub async fn kill_task(
    conn: &Surreal<Any>,
    id: &RecordId,
    reason: &str,
) -> Result<(), SurrealError> {
    let mut last_err = None;
    for attempt in 0..MAX_TX_RETRIES {
        let queried = with_timeout(
            "kill_task",
            conn.query(KILL_TASK)
                .bind(("id", id.clone()))
                .bind(("reason", reason.to_owned()))
                .into_future(),
        )
        .await;
        let Some(queried) = queried else {
            last_err = Some(timeout_error("kill_task"));
            timeout_backoff(attempt).await;
            continue;
        };
        let mut response = match queried {
            Ok(response) => response,
            Err(err) if is_retryable_transport(&err) => {
                last_err = Some(err);
                tx_unavailable_backoff("kill_task", attempt).await;
                continue;
            }
            Err(err) => return Err(SurrealError::Database(err)),
        };
        match classify_tx_errors(response.take_errors()) {
            Some(TxOutcome::Retry(err)) => {
                last_err = Some(err);
                tx_conflict_backoff("kill_task", attempt).await;
            }
            Some(TxOutcome::Unavailable(err)) => {
                last_err = Some(err);
                tx_unavailable_backoff("kill_task", attempt).await;
            }
            Some(TxOutcome::Fail(err)) => return Err(SurrealError::Database(err)),
            None => return Ok(()),
        }
    }
    // surface the failure after exhausting retries so the poison row is not silently left Queued
    Err(SurrealError::Database(last_err.unwrap_or_else(|| {
        surrealdb::Error::query(
            "Transaction conflict retries exhausted".to_owned(),
            QueryError::TransactionConflict,
        )
    })))
}
