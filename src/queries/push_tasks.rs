use std::future::IntoFuture;

use surrealdb::{
    Surreal,
    engine::any::Any,
    types::{AlreadyExistsError, Bytes, Datetime, QueryError, RecordId, SurrealValue},
};
use ulid::Ulid;

use crate::{
    CompactType, Config, JOB_TABLE, SurrealError, SurrealTask,
    queries::{
        MAX_TX_RETRIES, TxOutcome, classify_tx_errors, is_retryable_transport, timeout_error,
        tx_conflict_backoff, tx_unavailable_backoff, with_timeout,
    },
};

const PUSH_TASKS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/task/push_tasks.surql"
));

/// Table whose primary keys enforce idempotency
pub const DEDUPE_TABLE: &str = "job_dedupe";

#[derive(Clone, SurrealValue)]
struct PushRow {
    id: RecordId,
    dedupe_id: Option<RecordId>,
    args: Bytes,
    queue: String,
    max_attempts: i64,
    run_at: Datetime,
    priority: i64,
    metadata: serde_json::Value,
    idempotency_key: Option<String>,
}

fn is_record_exists(err: &surrealdb::Error) -> bool {
    if matches!(
        err.already_exists_details(),
        Some(AlreadyExistsError::Record { .. })
    ) {
        return true;
    }
    let message = err.message();
    message.contains("Database record") && message.contains("already exists")
}

/// Insert a batch of tasks in a single transaction, skipping duplicates by idempotency key
pub async fn push_tasks(
    conn: &Surreal<Any>,
    config: &Config,
    tasks: &[SurrealTask<CompactType>],
) -> Result<(), SurrealError> {
    let queue = config.queue().to_string();
    let rows: Vec<PushRow> = tasks
        .iter()
        .map(|task| {
            let id = task
                .parts
                .task_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| Ulid::new().to_string());
            let run_at = if task.parts.run_at == 0 {
                Datetime::now()
            } else {
                Datetime::from_timestamp(task.parts.run_at as i64, 0).unwrap_or_else(Datetime::now)
            };
            let dedupe_id = task
                .parts
                .idempotency_key
                .as_ref()
                .map(|key| RecordId::new(DEDUPE_TABLE, format!("{queue}::{key}")));
            PushRow {
                id: RecordId::new(JOB_TABLE, id),
                dedupe_id,
                args: Bytes::from(task.args.clone()),
                queue: queue.clone(),
                max_attempts: i64::from(task.parts.ctx.max_attempts()),
                run_at,
                priority: i64::from(task.parts.ctx.priority()),
                metadata: serde_json::Value::Object(task.parts.ctx.meta().clone()),
                idempotency_key: task.parts.idempotency_key.clone(),
            }
        })
        .collect();

    let mut last_err = None;
    for attempt in 0..MAX_TX_RETRIES {
        let queried = with_timeout(
            "push_tasks",
            conn.query(PUSH_TASKS)
                .bind(("tasks", rows.clone()))
                .into_future(),
        )
        .await;
        // the batch may have landed: resending could duplicate tasks without an idempotency key
        let Some(queried) = queried else {
            return Err(SurrealError::Database(timeout_error("push_tasks")));
        };
        let mut response = match queried {
            Ok(response) => response,
            Err(err) if is_retryable_transport(&err) => {
                last_err = Some(err);
                tx_unavailable_backoff("push_tasks", attempt).await;
                continue;
            }
            Err(err) => return Err(SurrealError::Database(err)),
        };
        match classify_tx_errors(response.take_errors()) {
            Some(TxOutcome::Retry(err)) => {
                last_err = Some(err);
                tx_conflict_backoff("push_tasks", attempt).await;
            }
            Some(TxOutcome::Unavailable(err)) => {
                last_err = Some(err);
                tx_unavailable_backoff("push_tasks", attempt).await;
            }
            Some(TxOutcome::Fail(err)) if is_record_exists(&err) => {
                last_err = Some(err);
                tx_conflict_backoff("push_tasks", attempt).await;
            }
            Some(TxOutcome::Fail(err)) => return Err(SurrealError::Database(err)),
            None => return Ok(()),
        }
    }
    // surface the failure after exhausting retries so the batch is not silently dropped
    Err(SurrealError::Database(last_err.unwrap_or_else(|| {
        surrealdb::Error::query(
            "Transaction conflict retries exhausted".to_owned(),
            QueryError::TransactionConflict,
        )
    })))
}
