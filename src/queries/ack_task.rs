use std::{future::IntoFuture, time::Duration};

use apalis_core::{
    error::{AbortError, BoxDynError},
    task::{Parts, status::Status},
};
use surrealdb::{
    Surreal,
    engine::any::Any,
    types::{QueryError, RecordId, Value},
};
use ulid::Ulid;

use crate::{
    JOB_TABLE, SurrealContext, SurrealError,
    queries::{
        MAX_TX_RETRIES, TxOutcome, classify_tx_errors, is_retryable_transport, timeout_backoff,
        timeout_error, tx_conflict_backoff, tx_unavailable_backoff, with_jitter, with_timeout,
    },
};

const ACK_TASK: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/task/ack_task.surql"
));

const ACK_TASK_RETRY: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/task/ack_task_retry.surql"
));

const RETRY_BACKOFF_BASE: Duration = Duration::from_secs(1);

const RETRY_BACKOFF_CAP: Duration = Duration::from_secs(300);

const RETRY_BACKOFF_MAX_SHIFT: u32 = 16;

fn retry_backoff(attempt: i64) -> Duration {
    let exp = u32::try_from(attempt.saturating_sub(1))
        .unwrap_or(0)
        .min(RETRY_BACKOFF_MAX_SHIFT);
    with_jitter(
        RETRY_BACKOFF_BASE
            .saturating_mul(2u32.saturating_pow(exp))
            .min(RETRY_BACKOFF_CAP),
    )
}

/// Record the outcome of a task, guarding on the owning worker
pub async fn ack_task(
    conn: &Surreal<Any>,
    task_id: &Ulid,
    worker: &str,
    result: serde_json::Value,
    status: &Status,
    attempt: i64,
) -> Result<(), SurrealError> {
    let id = RecordId::new(JOB_TABLE, task_id.to_string());
    let backoff_ms = matches!(status, Status::Failed)
        .then(|| i64::try_from(retry_backoff(attempt).as_millis()).unwrap_or(i64::MAX));

    let mut last_err = None;
    for attempt_no in 0..MAX_TX_RETRIES {
        let request = match backoff_ms {
            Some(ms) => conn.query(ACK_TASK_RETRY).bind(("backoff", ms)),
            None => conn.query(ACK_TASK).bind(("status", status.to_string())),
        }
        .bind(("id", id.clone()))
        .bind(("worker", worker.to_owned()))
        .bind(("attempts", attempt))
        .bind(("result", result.clone()));

        let queried = with_timeout("ack_task", request.into_future()).await;
        let Some(queried) = queried else {
            last_err = Some(timeout_error("ack_task"));
            timeout_backoff(attempt_no).await;
            continue;
        };
        let mut response = match queried {
            Ok(response) => response,
            Err(err) if is_retryable_transport(&err) => {
                last_err = Some(err);
                tx_unavailable_backoff("ack_task", attempt_no).await;
                continue;
            }
            Err(err) => return Err(SurrealError::Database(err)),
        };
        match classify_tx_errors(response.take_errors()) {
            Some(TxOutcome::Retry(err)) => {
                last_err = Some(err);
                tx_conflict_backoff("ack_task", attempt_no).await;
                continue;
            }
            Some(TxOutcome::Unavailable(err)) => {
                last_err = Some(err);
                tx_unavailable_backoff("ack_task", attempt_no).await;
                continue;
            }
            Some(TxOutcome::Fail(err)) => return Err(SurrealError::Database(err)),
            None => {}
        }
        let updated: Vec<Value> = response.take(0)?;
        if updated.is_empty() {
            if attempt_no == 0 {
                return Err(SurrealError::TaskNotFound(task_id.to_string()));
            }
            log::debug!(
                "Ack of task {task_id} matched no row on replay {attempt_no}: an earlier attempt \
                 already committed"
            );
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

/// Decide the terminal status from the task outcome and remaining attempts
pub fn calculate_status<Res>(
    parts: &Parts<SurrealContext, Ulid>,
    res: &Result<Res, BoxDynError>,
) -> Status {
    match res {
        Ok(_) => Status::Done,
        Err(e) => match e {
            _ if parts.ctx.max_attempts() as usize <= parts.attempt.current() => Status::Killed,
            e if e.downcast_ref::<AbortError>().is_some() => Status::Killed,
            _ => Status::Failed,
        },
    }
}
