use std::future::IntoFuture;

use surrealdb::{
    Surreal,
    engine::any::Any,
    types::{QueryError, RecordId, Value},
};
use ulid::Ulid;

use crate::{
    JOB_TABLE, SurrealError,
    queries::{
        MAX_TX_RETRIES, TxOutcome, classify_tx_errors, is_retryable_transport, timeout_backoff,
        timeout_error, tx_conflict_backoff, tx_unavailable_backoff, with_timeout,
    },
};

const LOCK_TASK: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/task/lock_task.surql"
));

/// Transition a claimed task to Running, guarding on the owning worker
pub async fn lock_task(
    conn: &Surreal<Any>,
    task_id: &Ulid,
    worker: &str,
) -> Result<(), SurrealError> {
    let id = RecordId::new(JOB_TABLE, task_id.to_string());

    let mut last_err = None;
    for attempt in 0..MAX_TX_RETRIES {
        let queried = with_timeout(
            "lock_task",
            conn.query(LOCK_TASK)
                .bind(("id", id.clone()))
                .bind(("worker", worker.to_owned()))
                .into_future(),
        )
        .await;
        let Some(queried) = queried else {
            last_err = Some(timeout_error("lock_task"));
            timeout_backoff(attempt).await;
            continue;
        };
        let mut response = match queried {
            Ok(response) => response,
            Err(err) if is_retryable_transport(&err) => {
                last_err = Some(err);
                tx_unavailable_backoff("lock_task", attempt).await;
                continue;
            }
            Err(err) => return Err(SurrealError::Database(err)),
        };
        match classify_tx_errors(response.take_errors()) {
            Some(TxOutcome::Retry(err)) => {
                last_err = Some(err);
                tx_conflict_backoff("lock_task", attempt).await;
                continue;
            }
            Some(TxOutcome::Unavailable(err)) => {
                last_err = Some(err);
                tx_unavailable_backoff("lock_task", attempt).await;
                continue;
            }
            Some(TxOutcome::Fail(err) | TxOutcome::CommitFailed(err)) => {
                return Err(SurrealError::Database(err));
            }
            None => {}
        }
        let updated: Vec<Value> = response.take(0)?;
        if updated.is_empty() {
            return Err(SurrealError::TaskNotFound(task_id.to_string()));
        }
        return Ok(());
    }
    Err(SurrealError::Database(last_err.unwrap_or_else(|| {
        surrealdb::Error::query(
            "Transaction conflict retries exhausted".to_owned(),
            QueryError::TransactionConflict,
        )
    })))
}
