use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use apalis_core::timer::Delay;
use futures::{
    future::{Either, select},
    pin_mut,
};
use surrealdb::types::{ConnectionError, QueryError};

/// Acknowledge a task on success or failure
pub mod ack_task;
/// Fetch a single task by its id
pub mod fetch_by_id;
/// Claim a batch of due tasks for a worker
pub mod fetch_next;
/// Claim due tasks across the queues of a shared storage
pub mod fetch_next_shared;
/// Keep workers alive by refreshing their heartbeat
pub mod keep_alive;
/// Park a task that can never run
pub mod kill_task;
/// List queues with their statistics
pub mod list_queues;
/// List tasks in a queue
pub mod list_tasks;
/// List workers registered with the backend
pub mod list_workers;
/// Lock a claimed task for processing
pub mod lock_task;
/// Collect queue and global metrics
pub mod metrics;
/// Insert a batch of tasks
pub mod push_tasks;
/// Re-enqueue tasks stranded by dead workers
pub mod reenqueue_orphaned;
/// Register a worker with the backend
pub mod register_worker;
/// Purge completed tasks from storage
pub mod vacuum;
/// Wait for tasks to reach a terminal state
pub mod wait_for;

pub(crate) const MAX_TX_RETRIES: usize = 8;

pub(crate) const CONSECUTIVE_FAILURES_ALARM: u32 = 3;

pub(crate) const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) async fn with_timeout<T>(op: &str, fut: impl Future<Output = T>) -> Option<T> {
    let timer = Delay::new(QUERY_TIMEOUT);
    pin_mut!(fut);
    pin_mut!(timer);
    match select(fut, timer).await {
        Either::Left((value, _)) => Some(value),
        Either::Right(_) => {
            log::warn!(
                "{op} did not answer within {QUERY_TIMEOUT:?}, abandoning the request; the \
                 connection is most likely reconnecting"
            );
            None
        }
    }
}

pub(crate) fn timeout_error(op: &str) -> surrealdb::Error {
    surrealdb::Error::query(
        format!("{op} timed out after {QUERY_TIMEOUT:?}"),
        QueryError::TimedOut {
            duration: QUERY_TIMEOUT,
        },
    )
}

const TX_CONFLICT_BASE: Duration = Duration::from_millis(50);

const TX_CONFLICT_CAP: Duration = Duration::from_secs(2);

pub(crate) fn with_jitter(base: Duration) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let fraction = f64::from(nanos) / f64::from(1_000_000_000u32);
    base.mul_f64(0.75 + fraction * 0.5)
}

/// First delay before retrying a transaction the datastore refused to start; doubled per attempt,
/// so the retries span a few seconds — long enough to ride out a PD/TSO re-election.
const TX_UNAVAILABLE_BASE: Duration = Duration::from_millis(250);

/// A datastore that cannot open a transaction (no timestamp from PD, storage unreachable) reports
/// this on every statement of the block, all flagged `NotExecuted`. The wording is misleading: the
/// connection has no transaction of its own, `BEGIN` simply never got one.
const TX_UNAVAILABLE: &str = "Tried to start a transaction while another transaction was open";

/// What to do with a transaction attempt
#[derive(Debug)]
pub(crate) enum TxOutcome {
    Retry(surrealdb::Error),
    /// The transaction never started, so nothing ran: retry once the datastore is back
    Unavailable(surrealdb::Error),
    /// Every statement ran but `COMMIT` failed for a reason other than a write conflict (key
    /// locked, region error). Nothing was committed, but only a caller whose block is safe to
    /// replay should retry it.
    CommitFailed(surrealdb::Error),
    Fail(surrealdb::Error),
}

fn unavailable_delay(attempt: usize) -> Duration {
    let exp = u32::try_from(attempt).unwrap_or(0).min(8);
    TX_UNAVAILABLE_BASE * 2u32.pow(exp)
}

/// Wait out a datastore that could not start the transaction
pub(crate) async fn tx_unavailable_backoff(op: &str, attempt: usize) {
    let delay = unavailable_delay(attempt);
    log::warn!("SurrealDB could not start the {op} transaction, retrying in {delay:?}");
    Delay::new(delay).await;
}

/// Wait before replaying a request that got no answer; `with_timeout` already logged it
pub(crate) async fn timeout_backoff(attempt: usize) {
    Delay::new(unavailable_delay(attempt)).await;
}

#[allow(dead_code)]
pub(crate) async fn tx_conflict_backoff(op: &str, attempt: usize) {
    let exp = u32::try_from(attempt).unwrap_or(u32::MAX).min(8);
    let delay = with_jitter(
        TX_CONFLICT_BASE
            .saturating_mul(2u32.saturating_pow(exp))
            .min(TX_CONFLICT_CAP),
    );
    log::debug!("Transaction conflict on {op}, replaying attempt {attempt} in {delay:?}");
    Delay::new(delay).await;
}

#[allow(dead_code)]
pub(crate) fn is_retryable_transport(err: &surrealdb::Error) -> bool {
    if matches!(
        err.connection_details(),
        Some(ConnectionError::ConnectionFailed | ConnectionError::Uninitialised)
    ) {
        return true;
    }
    let message = err.message().to_ascii_lowercase();
    [
        "connection",
        "not connected",
        "broken pipe",
        "closed",
        "timed out",
        "reset by peer",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

fn is_conflict(err: &surrealdb::Error) -> bool {
    matches!(err.query_details(), Some(QueryError::TransactionConflict))
        || err.message().contains("Transaction conflict")
}

fn is_not_executed(err: &surrealdb::Error) -> bool {
    matches!(err.query_details(), Some(QueryError::NotExecuted))
}

/// SurrealDB reports a failed `COMMIT` on a row of its own carrying this prefix, and flags every
/// earlier statement `NotExecuted`; that row is the only one holding the real cause.
const COMMIT_FAILED: &str = "Cannot COMMIT:";

fn is_commit_failure(err: &surrealdb::Error) -> bool {
    is_not_executed(err) && err.message().starts_with(COMMIT_FAILED)
}

fn is_tx_unavailable(err: &surrealdb::Error) -> bool {
    is_not_executed(err) && err.message().contains(TX_UNAVAILABLE)
}

/// Classify a BEGIN..COMMIT error set: siblings of a failed statement are `NotExecuted` noise, only the failing slot holds the real error
pub(crate) fn classify_tx_errors(errors: HashMap<usize, surrealdb::Error>) -> Option<TxOutcome> {
    if errors.is_empty() {
        return None;
    }
    // BTreeMap so the surfaced error is the lowest slot, not HashMap order
    let ordered: BTreeMap<usize, surrealdb::Error> = errors.into_iter().collect();
    let mut first_conflict = None;
    let mut first_real = None;
    let mut first_not_executed = None;
    let mut first_unavailable = None;
    let mut commit_failure = None;
    for (_, err) in ordered {
        if is_tx_unavailable(&err) {
            first_unavailable.get_or_insert(err);
        } else if is_commit_failure(&err) {
            commit_failure.get_or_insert(err);
        } else if is_conflict(&err) {
            first_conflict.get_or_insert(err);
        } else if is_not_executed(&err) {
            first_not_executed.get_or_insert(err);
        } else {
            first_real.get_or_insert(err);
        }
    }
    // nothing ran, so no other error can be the real cause
    if let Some(err) = first_unavailable {
        Some(TxOutcome::Unavailable(err))
    } else if let Some(err) = first_real {
        Some(TxOutcome::Fail(err))
    } else if let Some(err) = first_conflict {
        Some(TxOutcome::Retry(err))
    } else if let Some(err) = commit_failure {
        // the statements' NotExecuted rows only echo the commit; its own row says why
        Some(TxOutcome::CommitFailed(err))
    } else {
        // all NotExecuted with no commit row: fail rather than retry silently
        first_not_executed.map(TxOutcome::Fail)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    const ECHO: &str = "The query was not executed due to a failed transaction";

    fn not_executed(message: &str) -> surrealdb::Error {
        surrealdb::Error::query(message.to_owned(), QueryError::NotExecuted)
    }

    #[test]
    fn a_failed_commit_reports_the_commit_row_not_its_echoes() {
        let errors = HashMap::from([
            (0, not_executed(ECHO)),
            (1, not_executed(ECHO)),
            (2, not_executed("Cannot COMMIT: Key is locked")),
        ]);
        match classify_tx_errors(errors) {
            Some(TxOutcome::CommitFailed(err)) => {
                assert_eq!(err.message(), "Cannot COMMIT: Key is locked");
            }
            other => panic!("expected CommitFailed, got {other:?}"),
        }
    }

    #[test]
    fn a_commit_lost_to_a_write_conflict_stays_a_plain_retry() {
        let errors = HashMap::from([
            (0, not_executed(ECHO)),
            (
                1,
                surrealdb::Error::query(
                    "Cannot COMMIT: Transaction conflict".to_owned(),
                    QueryError::TransactionConflict,
                ),
            ),
        ]);
        assert!(matches!(
            classify_tx_errors(errors),
            Some(TxOutcome::Retry(_))
        ));
    }

    #[test]
    fn a_failing_statement_outranks_the_commit_it_aborted() {
        let errors = HashMap::from([
            (0, not_executed(ECHO)),
            (1, surrealdb::Error::thrown("bad field".to_owned())),
            (
                2,
                not_executed("Cannot COMMIT: the transaction was aborted due to a prior error"),
            ),
        ]);
        match classify_tx_errors(errors) {
            Some(TxOutcome::Fail(err)) => assert_eq!(err.message(), "bad field"),
            other => panic!("expected Fail, got {other:?}"),
        }
    }

    #[test]
    fn echoes_without_a_commit_row_still_fail() {
        let errors = HashMap::from([(0, not_executed(ECHO))]);
        assert!(matches!(
            classify_tx_errors(errors),
            Some(TxOutcome::Fail(_))
        ));
    }
}
