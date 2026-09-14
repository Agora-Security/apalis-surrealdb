use std::{collections::HashSet, str::FromStr, time::Duration};

use apalis_core::{
    backend::{BackendExt, TaskResult, WaitForCompletion},
    task::{status::Status, task_id::TaskId},
};
use futures::{StreamExt, stream, stream::BoxStream};
use serde::de::DeserializeOwned;
use surrealdb::types::{RecordId, SurrealValue};
use ulid::Ulid;

use crate::{CompactType, JOB_TABLE, SurrealContext, SurrealError, SurrealStorage};

const FETCH_COMPLETED_TASKS: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/fetch_completed_tasks.surql"
));

const POLL_INTERVAL: Duration = Duration::from_millis(500);

const MAX_CONSECUTIVE_FAILURES: u32 = 10;

#[derive(Debug, SurrealValue)]
struct ResultRow {
    id: String,
    status: String,
    result: Option<serde_json::Value>,
}

impl ResultRow {
    fn into_task_result<O>(self) -> Result<TaskResult<O, Ulid>, SurrealError>
    where
        Result<O, String>: DeserializeOwned,
    {
        let task_id = TaskId::from_str(&self.id).map_err(|e| SurrealError::Decode(Box::new(e)))?;
        let status = Status::from_str(&self.status).map_err(|e| SurrealError::Decode(e.into()))?;
        let result: Result<O, String> = match self.result {
            Some(value) => {
                serde_json::from_value(value).map_err(|e| SurrealError::Decode(e.into()))?
            }
            None => Err(format!(
                "task reached status {status} with no recorded result"
            )),
        };
        Ok(TaskResult::new(task_id, status, result))
    }
}

fn record_ids<'a>(ids: impl Iterator<Item = &'a String>) -> Vec<RecordId> {
    ids.map(|id| RecordId::new(JOB_TABLE, id.clone())).collect()
}

impl<O: 'static + Send, Args, D, F> WaitForCompletion<O> for SurrealStorage<Args, D, F>
where
    Self: BackendExt<
            Context = SurrealContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = SurrealError,
        >,
    Result<O, String>: DeserializeOwned,
{
    type ResultStream = BoxStream<'static, Result<TaskResult<O, Ulid>, SurrealError>>;

    fn wait_for(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>>,
    ) -> Self::ResultStream {
        let conn = self.conn.clone();
        let ids: HashSet<String> = task_ids.into_iter().map(|id| id.to_string()).collect();

        stream::unfold(
            (ids, 0u32, false),
            move |(mut remaining, mut failures, started)| {
                let conn = conn.clone();
                async move {
                    if remaining.is_empty() {
                        return None;
                    }
                    if started {
                        apalis_core::timer::sleep(POLL_INTERVAL).await;
                    }
                    let fetch = async {
                        let mut response = conn
                            .query(FETCH_COMPLETED_TASKS)
                            .bind(("ids", record_ids(remaining.iter())))
                            .await?;
                        response
                            .take::<Vec<ResultRow>>(0)
                            .map_err(SurrealError::from)
                    };
                    let rows = match fetch.await {
                        Ok(rows) => {
                            failures = 0;
                            rows
                        }
                        Err(e) => {
                            failures += 1;
                            if failures >= MAX_CONSECUTIVE_FAILURES {
                                log::error!(
                                    "Giving up waiting on {} task(s): {failures} consecutive failures fetching completed tasks, last error: {e}",
                                    remaining.len()
                                );
                                return Some((
                                    stream::iter(vec![Err(e)]),
                                    (HashSet::new(), failures, true),
                                ));
                            }
                            log::warn!(
                                "Failed to fetch completed tasks ({failures}/{MAX_CONSECUTIVE_FAILURES} consecutive): {e}. Retrying in {POLL_INTERVAL:?}."
                            );
                            return Some((stream::iter(Vec::new()), (remaining, failures, true)));
                        }
                    };
                    let results = rows
                        .into_iter()
                        .map(|row| {
                            remaining.remove(&row.id);
                            row.into_task_result()
                        })
                        .collect::<Vec<_>>();
                    Some((stream::iter(results), (remaining, failures, true)))
                }
            },
        )
        .flatten()
        .boxed()
    }

    fn check_status(
        &self,
        task_ids: impl IntoIterator<Item = TaskId<Self::IdType>> + Send,
    ) -> impl Future<Output = Result<Vec<TaskResult<O, Ulid>>, Self::Error>> + Send {
        let conn = self.conn.clone();
        let ids: Vec<String> = task_ids.into_iter().map(|id| id.to_string()).collect();
        async move {
            let mut response = conn
                .query(FETCH_COMPLETED_TASKS)
                .bind(("ids", record_ids(ids.iter())))
                .await?;
            let rows: Vec<ResultRow> = response.take(0)?;
            let results = rows
                .into_iter()
                .filter_map(|row| {
                    let id = row.id.clone();
                    match row.into_task_result() {
                        Ok(result) => Some(result),
                        Err(e) => {
                            log::warn!("Skipping task {id}: its result row failed to decode: {e}");
                            None
                        }
                    }
                })
                .collect();
            Ok(results)
        }
    }
}
