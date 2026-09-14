use std::future::IntoFuture;

use apalis_core::worker::context::WorkerContext;
use surrealdb::{Surreal, engine::any::Any, types::RecordId};

use crate::{
    Config, SurrealError, WORKER_TABLE,
    queries::{timeout_error, with_timeout},
};

const REGISTER_WORKER: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/register_worker.surql"
));

/// Register a worker instance
pub async fn register_worker(
    conn: &Surreal<Any>,
    config: &Config,
    worker: &WorkerContext,
    storage_name: &str,
    instance: &str,
) -> Result<(), SurrealError> {
    let id = RecordId::new(WORKER_TABLE, instance.to_owned());

    let Some(queried) = with_timeout(
        "register_worker",
        conn.query(REGISTER_WORKER)
            .bind(("worker", id))
            .bind(("name", worker.name().to_owned()))
            .bind(("queue", config.queue().to_string()))
            .bind(("storage", storage_name.to_owned()))
            .bind(("layers", worker.get_service().to_owned()))
            .bind(("instance", instance.to_owned()))
            .into_future(),
    )
    .await
    else {
        return Err(SurrealError::Database(timeout_error("register_worker")));
    };
    let mut response = queried?;

    if let Some(err) = response.take_errors().into_values().next() {
        return Err(SurrealError::Database(err));
    }
    Ok(())
}
