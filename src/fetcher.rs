use std::{
    collections::VecDeque,
    fmt,
    marker::PhantomData,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use apalis_core::{
    backend::poll_strategy::{PollContext, PollStrategyExt},
    timer::Delay,
    worker::context::WorkerContext,
};
use futures::{FutureExt, StreamExt, future::BoxFuture, stream::Stream};
use surrealdb::{
    Notification, Surreal,
    engine::any::Any,
    types::{Action, Value},
};

use crate::{
    CompactType, Config, JOB_TABLE, SurrealError, SurrealTask,
    queries::{fetch_next::fetch_next, with_jitter},
};

/// Marker fetcher that drives the polling backend for [`SurrealStorage`](crate::SurrealStorage)
#[derive(Clone, Debug)]
pub struct SurrealFetcher;

const FETCH_BACKOFF_BASE: Duration = Duration::from_millis(250);

const FETCH_BACKOFF_CAP: Duration = Duration::from_secs(30);

enum StreamState {
    Ready,
    Delay,
    Fetch(BoxFuture<'static, Result<Vec<SurrealTask<CompactType>>, SurrealError>>),
    Buffered(VecDeque<SurrealTask<CompactType>>),
    ErrorBackoff(Delay),
    Empty,
}

/// Poll-strategy driven fetcher that claims batches of due tasks from SurrealDB
#[pin_project::pin_project]
pub struct SurrealPollFetcher<Compact, Decode> {
    conn: Arc<Surreal<Any>>,
    config: Config,
    wrk: WorkerContext,
    instance: Arc<str>,
    _marker: PhantomData<(Compact, Decode)>,
    #[pin]
    state: StreamState,
    #[pin]
    delay_stream: Option<Pin<Box<dyn Stream<Item = ()> + Send>>>,
    prev_count: Arc<AtomicUsize>,
    consecutive_errors: u32,
}

impl<Compact, Decode> fmt::Debug for SurrealPollFetcher<Compact, Decode> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SurrealPollFetcher")
            .field("conn", &self.conn)
            .field("config", &self.config)
            .field("wrk", &self.wrk)
            .field("prev_count", &self.prev_count)
            .field("consecutive_errors", &self.consecutive_errors)
            .finish()
    }
}

impl<Compact, Decode> Clone for SurrealPollFetcher<Compact, Decode> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            config: self.config.clone(),
            wrk: self.wrk.clone(),
            instance: self.instance.clone(),
            _marker: PhantomData,
            state: StreamState::Ready,
            delay_stream: None,
            prev_count: Arc::new(AtomicUsize::new(0)),
            consecutive_errors: 0,
        }
    }
}

impl<Decode> SurrealPollFetcher<CompactType, Decode> {
    /// Create a poll fetcher that claims batches for the given worker
    #[must_use]
    pub fn new(
        conn: &Arc<Surreal<Any>>,
        config: &Config,
        wrk: &WorkerContext,
        instance: Arc<str>,
    ) -> Self {
        Self {
            conn: conn.clone(),
            config: config.clone(),
            wrk: wrk.clone(),
            instance,
            _marker: PhantomData,
            state: StreamState::Ready,
            delay_stream: None,
            prev_count: Arc::new(AtomicUsize::new(0)),
            consecutive_errors: 0,
        }
    }
}

impl<Decode> Stream for SurrealPollFetcher<CompactType, Decode> {
    type Item = Result<Option<SurrealTask<CompactType>>, SurrealError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.delay_stream.is_none() {
            let strategy = this
                .config
                .poll_strategy()
                .clone()
                .build_stream(&PollContext::new(this.wrk.clone(), this.prev_count.clone()));
            this.delay_stream = Some(Box::pin(strategy));
        }

        loop {
            match this.state {
                StreamState::Ready => {
                    let conn = this.conn.clone();
                    let config = this.config.clone();
                    let instance = this.instance.clone();
                    let fetch = async move { fetch_next(&conn, &config, &instance).await };
                    this.state = StreamState::Fetch(fetch.boxed());
                }
                StreamState::Delay => {
                    if let Some(delay_stream) = this.delay_stream.as_mut() {
                        match delay_stream.as_mut().poll_next(cx) {
                            Poll::Pending => return Poll::Pending,
                            Poll::Ready(Some(())) => this.state = StreamState::Ready,
                            Poll::Ready(None) => {
                                this.state = StreamState::Empty;
                                return Poll::Ready(None);
                            }
                        }
                    } else {
                        this.state = StreamState::Empty;
                        return Poll::Ready(None);
                    }
                }
                StreamState::Fetch(ref mut fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(tasks)) => {
                        this.consecutive_errors = 0;
                        this.prev_count.store(tasks.len(), Ordering::Relaxed);
                        if tasks.is_empty() {
                            this.state = StreamState::Delay;
                        } else {
                            this.state = StreamState::Buffered(tasks.into_iter().collect());
                        }
                    }
                    Poll::Ready(Err(e)) => {
                        this.consecutive_errors = this.consecutive_errors.saturating_add(1);
                        let failures = this.consecutive_errors;
                        let exp = failures.saturating_sub(1).min(16);
                        let backoff = with_jitter(
                            FETCH_BACKOFF_BASE
                                .saturating_mul(2u32.saturating_pow(exp))
                                .min(FETCH_BACKOFF_CAP),
                        );
                        log::error!(
                            "Failed to claim tasks from queue {}: {e}; {failures} consecutive \
                             failures, retrying in {backoff:?}",
                            this.config.queue()
                        );
                        this.state = StreamState::ErrorBackoff(Delay::new(backoff));
                        return Poll::Ready(Some(Ok(None)));
                    }
                },
                StreamState::Buffered(ref mut buffer) => {
                    if let Some(task) = buffer.pop_front() {
                        if buffer.is_empty() {
                            this.state = StreamState::Ready;
                        }
                        return Poll::Ready(Some(Ok(Some(task))));
                    }
                    this.state = StreamState::Ready;
                }
                StreamState::ErrorBackoff(ref mut delay) => match delay.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => this.state = StreamState::Ready,
                },
                StreamState::Empty => return Poll::Ready(None),
            }
        }
    }
}

impl<Compact, Decode> SurrealPollFetcher<Compact, Decode> {
    /// Drain tasks already claimed but not yet yielded
    pub fn take_pending(&mut self) -> VecDeque<SurrealTask<CompactType>> {
        match &mut self.state {
            StreamState::Buffered(tasks) => std::mem::take(tasks),
            _ => VecDeque::new(),
        }
    }
}

type NotificationStream =
    Pin<Box<dyn Stream<Item = Result<Notification<Value>, surrealdb::Error>> + Send + Sync>>;
type SubscribeFuture = Pin<
    Box<
        dyn Future<Output = Result<(Arc<Surreal<Any>>, NotificationStream), surrealdb::Error>>
            + Send
            + Sync,
    >,
>;

const BACKOFF_BASE: Duration = Duration::from_secs(1);
const BACKOFF_CAP: Duration = Duration::from_secs(30);

enum LiveState {
    Init,
    Subscribing(SubscribeFuture),
    // the subscribing client is held so its session, and the live query, outlive the notification stream
    Active(#[allow(dead_code)] Arc<Surreal<Any>>, NotificationStream),
    Backoff(Delay),
}

/// Live-query fetcher that wakes the backend when a task is created on the `job` table
pub struct SurrealLiveFetcher {
    conn: Arc<Surreal<Any>>,
    state: LiveState,
    backoff: Duration,
}

impl SurrealLiveFetcher {
    /// Create a live-query fetcher over the given connection
    #[must_use]
    pub fn new(conn: &Arc<Surreal<Any>>) -> Self {
        Self {
            conn: conn.clone(),
            state: LiveState::Init,
            backoff: BACKOFF_BASE,
        }
    }
}

impl fmt::Debug for SurrealLiveFetcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SurrealLiveFetcher")
            .field("conn", &self.conn)
            .finish()
    }
}

impl Clone for SurrealLiveFetcher {
    fn clone(&self) -> Self {
        Self::new(&self.conn)
    }
}

impl Stream for SurrealLiveFetcher {
    type Item = ();

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match &mut this.state {
                LiveState::Init => {
                    let conn = this.conn.clone();
                    let subscribe = async move {
                        let stream: surrealdb::Stream<Vec<Value>> =
                            conn.select(JOB_TABLE).live().await?;
                        Ok((conn, Box::pin(stream) as NotificationStream))
                    };
                    this.state = LiveState::Subscribing(Box::pin(subscribe));
                }
                LiveState::Subscribing(fut) => match fut.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok((conn, stream))) => {
                        this.state = LiveState::Active(conn, stream);
                    }
                    Poll::Ready(Err(e)) => {
                        log::error!(
                            "Failed to subscribe to live query on {JOB_TABLE}: {e}; \
                             real-time delivery degraded, retrying in {:?} while polling continues",
                            this.backoff
                        );
                        this.state = LiveState::Backoff(Delay::new(this.backoff));
                        this.backoff = (this.backoff * 2).min(BACKOFF_CAP);
                    }
                },
                LiveState::Active(_, stream) => match stream.poll_next_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Some(Ok(n))) => {
                        this.backoff = BACKOFF_BASE;
                        if n.action == Action::Create {
                            return Poll::Ready(Some(()));
                        }
                    }
                    Poll::Ready(Some(Err(e))) => log::warn!("Live query notification error: {e}"),
                    Poll::Ready(None) => {
                        log::warn!(
                            "Live query on {JOB_TABLE} ended, resubscribing in {:?}",
                            this.backoff
                        );
                        this.state = LiveState::Backoff(Delay::new(this.backoff));
                        this.backoff = (this.backoff * 2).min(BACKOFF_CAP);
                    }
                },
                LiveState::Backoff(delay) => match delay.poll_unpin(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(()) => this.state = LiveState::Init,
                },
            }
        }
    }
}
