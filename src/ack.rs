use apalis_core::{
    error::BoxDynError,
    layers::{Layer, Service},
    task::Parts,
    worker::ext::ack::Acknowledge,
};
use futures::{FutureExt, future::BoxFuture};
use serde::Serialize;
use std::sync::Arc;
use surrealdb::{Surreal, engine::any::Any};
use ulid::Ulid;

use crate::{
    SurrealContext, SurrealError, SurrealTask,
    queries::{
        ack_task::{ack_task, calculate_status},
        lock_task::lock_task,
    },
};

#[derive(Clone, Debug)]
pub struct SurrealAck {
    conn: Arc<Surreal<Any>>,
}

impl SurrealAck {
    #[must_use]
    pub fn new(conn: Arc<Surreal<Any>>) -> Self {
        Self { conn }
    }
}

impl<Res: Serialize + 'static> Acknowledge<Res, SurrealContext, Ulid> for SurrealAck {
    type Error = SurrealError;
    type Future = BoxFuture<'static, Result<(), Self::Error>>;

    fn ack(
        &mut self,
        res: &Result<Res, BoxDynError>,
        parts: &Parts<SurrealContext, Ulid>,
    ) -> Self::Future {
        let task_id = parts.task_id.map(|id| *id.inner());
        let worker = parts.ctx.lock_by().clone();
        let result = serde_json::to_value(res.as_ref().map_err(ToString::to_string));
        let status = calculate_status(parts, res);
        parts.status.store(status.clone());
        let attempt = parts.attempt.current() as i64;
        let conn = self.conn.clone();
        async move {
            let task_id = task_id.ok_or(SurrealError::MissingTaskId)?;
            let worker = worker.ok_or(SurrealError::MissingWorkerContext)?;
            let result = match result {
                Ok(value) => value,
                Err(e) => {
                    log::error!(
                        "Could not serialise the result of task {task_id}: {e}. Recording a placeholder error so the task still reaches a terminal state."
                    );
                    serde_json::json!({ "Err": format!("<result could not be serialised: {e}>") })
                }
            };
            if let Err(e) = ack_task(&conn, &task_id, &worker, result, &status, attempt).await {
                if matches!(e, SurrealError::TaskNotFound(_)) {
                    log::error!(
                        "Discarded the result of task {task_id} (status {status}): the `lock_by = {worker}` guard did not match, so instance {worker} lost the lock while running. The task now belongs to another worker and will be, or already has been, re-run there."
                    );
                }
                return Err(e);
            }
            Ok(())
        }
        .boxed()
    }
}

#[derive(Clone, Debug)]
pub struct LockTaskLayer {
    conn: Arc<Surreal<Any>>,
    instance: Arc<str>,
}

impl LockTaskLayer {
    #[must_use]
    pub fn new(conn: Arc<Surreal<Any>>, instance: Arc<str>) -> Self {
        Self { conn, instance }
    }
}

impl<S> Layer<S> for LockTaskLayer {
    type Service = LockTaskService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        LockTaskService {
            inner,
            conn: self.conn.clone(),
            instance: self.instance.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LockTaskService<S> {
    inner: S,
    conn: Arc<Surreal<Any>>,
    instance: Arc<str>,
}

impl<S, Args> Service<SurrealTask<Args>> for LockTaskService<S>
where
    S: Service<SurrealTask<Args>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxDynError>,
    Args: Send + 'static,
{
    type Response = S::Response;
    type Error = BoxDynError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, mut req: SurrealTask<Args>) -> Self::Future {
        let conn = self.conn.clone();
        let worker = self.instance.to_string();
        let task_id = req.parts.task_id.map(|id| *id.inner());
        let Some(task_id) = task_id else {
            return async { Err(SurrealError::MissingTaskId.into()) }.boxed();
        };
        req.parts.ctx = req.parts.ctx.with_lock_by(Some(worker.clone()));
        let fut = self.inner.call(req);
        async move {
            match lock_task(&conn, &task_id, &worker).await {
                Ok(()) => fut.await.map_err(Into::into),
                Err(e) => {
                    drop(fut);
                    log::warn!(
                        "Refusing to run task {task_id}: could not take the lock for instance {worker}: {e}. Another worker may already own it; it will be reclaimed by orphan recovery if not."
                    );
                    Err(e.into())
                }
            }
        }
        .boxed()
    }
}
