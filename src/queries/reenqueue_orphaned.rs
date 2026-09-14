use std::future::IntoFuture;
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;

use apalis_core::timer::Delay;
use futures::{Stream, stream};
use surrealdb::{IndexedResults, Surreal, engine::any::Any, types::Value};

use crate::{
    Config, SurrealError,
    queries::{
        CONSECUTIVE_FAILURES_ALARM, MAX_TX_RETRIES, TxOutcome, classify_tx_errors,
        is_retryable_transport, tx_conflict_backoff, tx_unavailable_backoff, with_jitter,
        with_timeout,
    },
};

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

fn count_at(response: &mut IndexedResults, index: usize) -> usize {
    match response.take::<Vec<Value>>(index) {
        Ok(rows) => rows.len(),
        Err(e) => {
            log::debug!("Could not read the reenqueue_orphaned row count at slot {index}: {e}");
            0
        }
    }
}

/// Return tasks held by timed-out workers to the queue, killing those out of attempts, and report
/// how many were re-enqueued
pub async fn reenqueue_orphaned(
    conn: &Arc<Surreal<Any>>,
    config: &Config,
) -> Result<u64, SurrealError> {
    let dead_for = effective_reenqueue_millis(config);
    let reap_after = dead_for.saturating_mul(4);
    let queue = config.queue().to_string();

    let mut last_err = None;
    for attempt in 0..MAX_TX_RETRIES {
        let queried = with_timeout(
            "reenqueue_orphaned",
            conn.query(REENQUEUE_ORPHANED)
                .bind(("queue", queue.clone()))
                .bind(("dur", dead_for))
                .bind(("reap", reap_after))
                .into_future(),
        )
        .await;
        let Some(queried) = queried else {
            tx_unavailable_backoff("reenqueue_orphaned", attempt).await;
            continue;
        };
        let mut response = match queried {
            Ok(response) => response,
            Err(err) if is_retryable_transport(&err) => {
                last_err = Some(err);
                tx_unavailable_backoff("reenqueue_orphaned", attempt).await;
                continue;
            }
            Err(err) => return Err(SurrealError::Database(err)),
        };
        match classify_tx_errors(response.take_errors()) {
            Some(TxOutcome::Retry(err)) => {
                last_err = Some(err);
                tx_conflict_backoff("reenqueue_orphaned", attempt).await;
                continue;
            }
            Some(TxOutcome::Unavailable(err)) => {
                last_err = Some(err);
                tx_unavailable_backoff("reenqueue_orphaned", attempt).await;
                continue;
            }
            Some(TxOutcome::Fail(err)) => return Err(SurrealError::Database(err)),
            None => {}
        }

        let claimed_retriable = count_at(&mut response, 8);
        let claimed_killed = count_at(&mut response, 9);
        let running_retriable = count_at(&mut response, 10);
        let running_killed = count_at(&mut response, 11);

        if claimed_retriable > 0 {
            log::info!(
                "Returned {claimed_retriable} claimed-but-unstarted tasks to queue {queue}; \
                 their worker stopped reporting before they ran, so no attempt was charged"
            );
        }
        if running_retriable > 0 {
            log::info!(
                "Re-enqueued {running_retriable} interrupted tasks on queue {queue} \
                 that were running under a worker that is no longer live"
            );
        }
        if claimed_killed > 0 {
            log::warn!(
                "Killed {claimed_killed} tasks on queue {queue} that were claimed by a worker \
                 that stopped reporting and had no attempts left"
            );
        }
        if running_killed > 0 {
            log::warn!(
                "Killed {running_killed} tasks on queue {queue} whose worker stopped reporting \
                 mid-run with no attempts remaining"
            );
        }

        return Ok((claimed_retriable + running_retriable) as u64);
    }
    Err(SurrealError::Database(last_err.unwrap_or_else(|| {
        surrealdb::Error::query(
            "Transaction conflict retries exhausted".to_owned(),
            surrealdb::types::QueryError::TransactionConflict,
        )
    })))
}

/// Re-enqueue orphaned tasks at a fixed interval
pub fn reenqueue_orphaned_stream(
    conn: Arc<Surreal<Any>>,
    config: Config,
    interval: Duration,
) -> impl Stream<Item = Result<u64, SurrealError>> + Send {
    stream::unfold(0u32, move |failures: u32| {
        let conn = conn.clone();
        let config = config.clone();
        async move {
            Delay::new(with_jitter(interval)).await;
            let (count, failures) = match reenqueue_orphaned(&conn, &config).await {
                Ok(count) => (count, 0),
                Err(e) => {
                    let failures = failures.saturating_add(1);
                    if failures >= CONSECUTIVE_FAILURES_ALARM {
                        log::error!(
                            "Failed to re-enqueue orphaned tasks for queue {}: {e}; \
                             {failures} consecutive failures, stranded tasks are not being recovered",
                            config.queue()
                        );
                    } else {
                        log::warn!(
                            "Failed to re-enqueue orphaned tasks for queue {}: {e}",
                            config.queue()
                        );
                    }
                    (0, failures)
                }
            };
            Some((Ok(count), failures))
        }
    })
}
