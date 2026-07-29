use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;

use apalis_core::timer::Delay;
use futures::{Stream, stream};
use surrealdb::{Surreal, engine::any::Any, types::Value};

use crate::{Config, SurrealError};

const REENQUEUE_ORPHANED: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/reenqueue_orphaned.surql"
));

static CLAMP_WARN: Once = Once::new();

/// Liveness window in millis, floored at 2 * keep_alive so a worker mid-heartbeat is never judged dead
fn effective_reenqueue_millis(config: &Config) -> i64 {
    let after = config.reenqueue_orphaned_after();
    let floor = config.keep_alive().saturating_mul(2);
    if after < floor {
        CLAMP_WARN.call_once(|| {
            log::warn!(
                "reenqueue_orphaned_after ({after:?}) < 2 * keep_alive ({:?}); clamping to {floor:?}",
                *config.keep_alive()
            );
        });
        return floor.as_millis() as i64;
    }
    after.as_millis() as i64
}

/// Return tasks held by timed-out workers to the queue, killing those out of attempts, and report
/// how many were re-enqueued
pub async fn reenqueue_orphaned(
    conn: &Arc<Surreal<Any>>,
    config: &Config,
) -> Result<u64, SurrealError> {
    let dead_for = effective_reenqueue_millis(config);
    let mut response = conn
        .query(REENQUEUE_ORPHANED)
        .bind(("queue", config.queue().to_string()))
        .bind(("dur", dead_for))
        .await?;
    let killed: Vec<Value> = response.take(4)?;
    let reenqueued: Vec<Value> = response.take(5)?;
    if !killed.is_empty() {
        log::warn!(
            "Killed {} orphaned tasks that had no attempts left",
            killed.len()
        );
    }
    let count = reenqueued.len() as u64;
    if count > 0 {
        log::info!("Re-enqueued {count} orphaned tasks that were being processed by dead workers");
    }
    Ok(count)
}

/// Re-enqueue orphaned tasks at a fixed interval
pub fn reenqueue_orphaned_stream(
    conn: Arc<Surreal<Any>>,
    config: Config,
    interval: Duration,
) -> impl Stream<Item = Result<u64, SurrealError>> + Send {
    stream::unfold((), move |()| {
        let conn = conn.clone();
        let config = config.clone();
        async move {
            Delay::new(interval).await;
            Some((reenqueue_orphaned(&conn, &config).await, ()))
        }
    })
}
