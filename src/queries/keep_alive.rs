use apalis_core::{timer::Delay, worker::context::WorkerContext};
use futures::{Stream, stream};
use std::future::IntoFuture;
use std::sync::Arc;
use std::time::{Duration, Instant};
use surrealdb::{
    Surreal,
    engine::any::Any,
    types::{RecordId, Value},
};

use crate::{
    Config, SurrealError, WORKER_TABLE,
    queries::{
        CONSECUTIVE_FAILURES_ALARM, reenqueue_orphaned::reenqueue_orphaned,
        register_worker::register_worker, timeout_error, with_timeout,
    },
};

const FATAL_AFTER: Duration = Duration::from_secs(5 * 60);

const KEEP_ALIVE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/keep_alive.surql"
));

/// Refresh the worker heartbeat so it is not treated as orphaned
pub async fn keep_alive(
    conn: &Arc<Surreal<Any>>,
    config: &Config,
    worker: &WorkerContext,
    storage_name: &str,
    instance: &str,
) -> Result<(), SurrealError> {
    let id = RecordId::new(WORKER_TABLE, instance.to_owned());
    let Some(queried) = with_timeout(
        "keep_alive",
        conn.query(KEEP_ALIVE).bind(("worker", id)).into_future(),
    )
    .await
    else {
        return Err(SurrealError::Database(timeout_error("keep_alive")));
    };
    let mut response = queried?;
    let updated: Vec<Value> = response.take(0)?;
    if updated.is_empty() {
        log::warn!(
            "Worker row for {} (instance {instance}) is gone, re-registering it",
            worker.name()
        );
        register_worker(conn, config, worker, storage_name, instance).await?;
    }
    Ok(())
}

/// Re-enqueue orphaned tasks then register the worker
pub async fn initial_heartbeat(
    conn: &Arc<Surreal<Any>>,
    config: &Config,
    worker: &WorkerContext,
    storage_name: &str,
    instance: &str,
) -> Result<(), SurrealError> {
    reenqueue_orphaned(conn, config).await?;
    register_worker(conn, config, worker, storage_name, instance).await?;
    Ok(())
}

/// How long the heartbeat has been missing, carried across beats. `failing_since` is what decides
/// the worker's fate; `failures` only picks the log line.
#[derive(Default)]
struct State {
    failures: u32,
    failing_since: Option<Instant>,
}

/// Emit a keep-alive at every keep-alive interval
pub fn keep_alive_stream(
    conn: Arc<Surreal<Any>>,
    config: Config,
    worker: WorkerContext,
    storage_name: &'static str,
    instance: Arc<str>,
) -> impl Stream<Item = Result<(), SurrealError>> + Send {
    stream::unfold(State::default(), move |state: State| {
        let conn = conn.clone();
        let config = config.clone();
        let worker = worker.clone();
        let instance = instance.clone();
        async move {
            Delay::new(*config.keep_alive()).await;
            match keep_alive(&conn, &config, &worker, storage_name, &instance).await {
                Ok(()) => Some((Ok(()), State::default())),
                Err(e) => {
                    let failures = state.failures.saturating_add(1);
                    let since = state.failing_since.unwrap_or_else(Instant::now);
                    let name = worker.name();
                    let down_for = since.elapsed();
                    if down_for >= FATAL_AFTER {
                        log::error!(
                            "keep-alive for worker {name} (instance {instance}) has not landed for \
                             {down_for:?}: {e}; ending the worker so its connection is rebuilt"
                        );
                        return Some((Err(e), State::default()));
                    }
                    if failures >= CONSECUTIVE_FAILURES_ALARM {
                        log::error!(
                            "keep-alive failed for worker {name} (instance {instance}): {e}; \
                             {failures} consecutive failures, this worker may be re-enqueued as orphaned"
                        );
                    } else {
                        log::warn!(
                            "keep-alive failed for worker {name} (instance {instance}): {e}"
                        );
                    }
                    Some((
                        Ok(()),
                        State {
                            failures,
                            failing_since: Some(since),
                        },
                    ))
                }
            }
        }
    })
}
