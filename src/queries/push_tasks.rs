use surrealdb::{
    Surreal,
    engine::any::Any,
    types::{Bytes, Datetime, QueryError, RecordId, SurrealValue},
};
use ulid::Ulid;

use crate::{
    CompactType, Config, JOB_TABLE, SurrealError, SurrealTask,
    queries::{MAX_TX_RETRIES, TxOutcome, classify_tx_errors, tx_unavailable_backoff},
};

const PUSH_TASKS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/task/push_tasks.surql"
));

#[derive(Clone, SurrealValue)]
struct PushRow {
    id: RecordId,
    args: Bytes,
    queue: String,
    max_attempts: i64,
    run_at: Datetime,
    priority: i64,
    metadata: serde_json::Value,
    idempotency_key: Option<String>,
}

/// Insert a batch of tasks in a single transaction, skipping duplicates by idempotency key
pub async fn push_tasks(
    conn: &Surreal<Any>,
    config: &Config,
    tasks: Vec<SurrealTask<CompactType>>,
) -> Result<(), SurrealError> {
    let queue = config.queue().to_string();
    let rows: Vec<PushRow> = tasks
        .into_iter()
        .map(|task| {
            let id = task
                .parts
                .task_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| Ulid::new().to_string());
            let run_at =
                Datetime::from_timestamp(task.parts.run_at as i64, 0).unwrap_or_else(Datetime::now);
            PushRow {
                id: RecordId::new(JOB_TABLE, id),
                args: Bytes::from(task.args),
                queue: queue.clone(),
                max_attempts: i64::from(task.parts.ctx.max_attempts()),
                run_at,
                priority: i64::from(task.parts.ctx.priority()),
                metadata: serde_json::Value::Object(task.parts.ctx.meta().clone()),
                idempotency_key: task.parts.idempotency_key,
            }
        })
        .collect();

    let mut last_err = None;
    for attempt in 0..MAX_TX_RETRIES {
        let mut response = conn.query(PUSH_TASKS).bind(("tasks", rows.clone())).await?;
        match classify_tx_errors(response.take_errors()) {
            Some(TxOutcome::Retry(err)) => last_err = Some(err),
            Some(TxOutcome::Unavailable(err)) => {
                last_err = Some(err);
                tx_unavailable_backoff("push_tasks", attempt).await;
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
