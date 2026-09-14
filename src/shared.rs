use std::{
    collections::{HashMap, hash_map::Entry},
    fmt,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{Backend, BackendExt, TaskStream, codec::Codec, queue::Queue, shared::MakeShared},
    layers::Stack,
    timer::Delay,
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use futures::{
    FutureExt, Stream, StreamExt,
    channel::mpsc::{self, Receiver, Sender},
    future::{BoxFuture, Shared},
    lock::Mutex,
    stream::{self, BoxStream, select},
};
use surrealdb::{Surreal, engine::any::Any};
use ulid::Ulid;

use crate::{
    CompactType, Config, LockTaskLayer, SurrealAck, SurrealContext, SurrealError,
    SurrealLiveFetcher, SurrealPollFetcher, SurrealStorage, SurrealTask,
    queries::fetch_next_shared::fetch_next_shared, sink::SurrealSink,
};

type Registry = Arc<Mutex<HashMap<String, Sender<SurrealTask<CompactType>>>>>;

/// A SurrealDB storage whose single connection is shared across workers of different queues
pub struct SharedSurrealStorage<Decode> {
    conn: Arc<Surreal<Any>>,
    registry: Registry,
    drive: Shared<BoxFuture<'static, ()>>,
    _marker: PhantomData<Decode>,
}

impl<Decode> Clone for SharedSurrealStorage<Decode> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            registry: self.registry.clone(),
            drive: self.drive.clone(),
            _marker: PhantomData,
        }
    }
}

impl<Decode> fmt::Debug for SharedSurrealStorage<Decode> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedSurrealStorage")
            .field("conn", &self.conn)
            .field("codec", &std::any::type_name::<Decode>())
            .finish()
    }
}

impl<Decode> SharedSurrealStorage<Decode> {
    /// Get the SurrealDB client shared across the registered workers
    #[must_use]
    pub fn client(&self) -> &Surreal<Any> {
        &self.conn
    }
}

impl SharedSurrealStorage<JsonCodec<CompactType>> {
    /// Create a shared storage over an existing SurrealDB connection
    #[must_use]
    pub fn new(conn: &Arc<Surreal<Any>>) -> Self {
        Self::new_with_codec(conn)
    }

    /// Create a shared storage over an existing connection using a specific codec
    #[must_use]
    pub fn new_with_codec<C>(conn: &Arc<Surreal<Any>>) -> SharedSurrealStorage<C> {
        let registry: Registry = Arc::new(Mutex::new(HashMap::new()));
        let fallback = *Config::default().keep_alive();
        let drive = drive_registry(conn.clone(), registry.clone(), fallback)
            .boxed()
            .shared();
        SharedSurrealStorage {
            conn: conn.clone(),
            registry,
            drive,
            _marker: PhantomData,
        }
    }
}

const WAKE_CHUNK: usize = 1;

const MIN_CLAIM_BATCH: usize = 10;

// fan claimed tasks out to the perqueue channels, waking on live notifications or a fallback tick
async fn drive_registry(conn: Arc<Surreal<Any>>, registry: Registry, fallback: Duration) {
    let live = SurrealLiveFetcher::new(&conn);
    let ticks = stream::unfold((), move |()| async move {
        Delay::new(fallback).await;
        Some(((), ()))
    });
    select(live, ticks)
        .ready_chunks(WAKE_CHUNK)
        .for_each(move |_| {
            let conn = conn.clone();
            let registry = registry.clone();
            async move {
                let mut senders: HashMap<String, Sender<SurrealTask<CompactType>>> =
                    registry.lock().await.clone();
                if senders.is_empty() {
                    return;
                }
                let queues: Vec<String> = senders.keys().cloned().collect();
                let limit = std::cmp::max(MIN_CLAIM_BATCH, queues.len()) as i64;
                let tasks = match fetch_next_shared(&conn, &queues, limit).await {
                    Ok(tasks) => tasks,
                    Err(err) => {
                        log::error!("Shared driver failed to claim tasks: {err}");
                        return;
                    }
                };
                let mut dead: Vec<String> = Vec::new();
                for task in tasks {
                    let Some(queue) = task.parts.ctx.queue().clone() else {
                        continue;
                    };
                    let Some(tx) = senders.get_mut(&queue) else {
                        continue;
                    };
                    let disconnected = match tx.try_send(task) {
                        Ok(()) => false,
                        Err(err) if err.is_full() => {
                            log::debug!("Shared driver: queue {queue} is saturated, task deferred");
                            drop(err.into_inner());
                            false
                        }
                        Err(err) => {
                            log::warn!(
                                "Shared driver: queue {queue} has no receiver, unregistering"
                            );
                            drop(err.into_inner());
                            true
                        }
                    };
                    if disconnected {
                        senders.remove(&queue);
                        dead.push(queue);
                    }
                }
                if !dead.is_empty() {
                    let mut registry = registry.lock().await;
                    for queue in dead {
                        registry.remove(&queue);
                    }
                }
            }
        })
        .await;
}

#[derive(Debug, thiserror::Error)]
pub enum SharedSurrealError {
    #[error("Namespace {0} already exists")]
    NamespaceExists(String),
    #[error("Could not acquire registry lock")]
    RegistryLocked,
}

impl<Args, Decode: Codec<Args, Compact = CompactType>> MakeShared<Args>
    for SharedSurrealStorage<Decode>
{
    type Backend = SurrealStorage<Args, Decode, SharedFetcher<CompactType>>;
    type Config = Config;
    type MakeError = SharedSurrealError;

    fn make_shared(&mut self) -> Result<Self::Backend, Self::MakeError>
    where
        Self::Config: Default,
    {
        self.make_shared_with_config(Config::new(std::any::type_name::<Args>()))
    }

    fn make_shared_with_config(
        &mut self,
        config: Self::Config,
    ) -> Result<Self::Backend, Self::MakeError> {
        let (tx, rx) = mpsc::channel(config.buffer_size());
        let mut registry = self
            .registry
            .try_lock()
            .ok_or(SharedSurrealError::RegistryLocked)?;
        // a plain insert would clobber the registered channel before the duplicate is rejected
        match registry.entry(config.queue().to_string()) {
            Entry::Occupied(_) => {
                return Err(SharedSurrealError::NamespaceExists(
                    config.queue().to_string(),
                ));
            }
            Entry::Vacant(entry) => {
                entry.insert(tx);
            }
        }
        let sink = SurrealSink::new(&self.conn, &config);
        Ok(SurrealStorage {
            conn: self.conn.clone(),
            instance: crate::new_instance(),
            storage_name: "SharedSurrealStorage",
            job_type: PhantomData,
            codec: PhantomData,
            config,
            sink,
            fetcher: SharedFetcher {
                poller: self.drive.clone(),
                receiver: Arc::new(std::sync::Mutex::new(rx)),
            },
        })
    }
}

/// Fetcher that drives the shared registry and yields the tasks routed to one queue
pub struct SharedFetcher<Compact> {
    poller: Shared<BoxFuture<'static, ()>>,
    receiver: Arc<std::sync::Mutex<Receiver<SurrealTask<Compact>>>>,
}

impl<Compact> Clone for SharedFetcher<Compact> {
    fn clone(&self) -> Self {
        Self {
            poller: self.poller.clone(),
            receiver: self.receiver.clone(),
        }
    }
}

impl<Compact> fmt::Debug for SharedFetcher<Compact> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedFetcher").finish()
    }
}

impl<Compact> Stream for SharedFetcher<Compact> {
    type Item = SurrealTask<Compact>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        // polling the shared driver keeps it running for every registered queue
        let _ = this.poller.poll_unpin(cx);
        match this.receiver.lock() {
            Ok(mut receiver) => receiver.poll_next_unpin(cx),
            Err(_) => Poll::Ready(None),
        }
    }
}

impl<Args, Decode> Backend for SurrealStorage<Args, Decode, SharedFetcher<CompactType>>
where
    Args: Send + Sync + Unpin + 'static,
    Decode: Codec<Args, Compact = CompactType> + Send + Sync + Unpin + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;
    type IdType = Ulid;
    type Context = SurrealContext;
    type Error = SurrealError;
    type Stream = TaskStream<SurrealTask<Args>, SurrealError>;
    type Beat = BoxStream<'static, Result<(), SurrealError>>;
    type Layer = Stack<AcknowledgeLayer<SurrealAck>, LockTaskLayer>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        self.heartbeat_stream(worker)
    }

    fn middleware(&self) -> Self::Layer {
        let lock = LockTaskLayer::new(self.conn.clone(), self.instance.clone());
        let ack = AcknowledgeLayer::new(SurrealAck::new(self.conn.clone()));
        Stack::new(ack, lock)
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let conn = self.conn.clone();
        self.poll_shared(worker)
            .then(move |a| {
                let conn = conn.clone();
                async move { crate::decode_or_kill::<Args, Decode>(&conn, a).await }
            })
            .boxed()
    }
}

impl<Args, Decode: Send + 'static> BackendExt
    for SurrealStorage<Args, Decode, SharedFetcher<CompactType>>
where
    Self: Backend<Args = Args, IdType = Ulid, Context = SurrealContext, Error = SurrealError>,
    Decode: Codec<Args, Compact = CompactType> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
    Args: Send + Unpin + 'static,
{
    type Codec = Decode;
    type Compact = CompactType;
    type CompactStream = TaskStream<SurrealTask<CompactType>, SurrealError>;

    fn get_queue(&self) -> Queue {
        self.config.queue().clone()
    }

    fn poll_compact(self, worker: &WorkerContext) -> Self::CompactStream {
        self.poll_shared(worker).boxed()
    }
}

impl<Args, Decode: Send + 'static> SurrealStorage<Args, Decode, SharedFetcher<CompactType>> {
    fn poll_shared(
        self,
        worker: &WorkerContext,
    ) -> impl Stream<Item = Result<Option<SurrealTask<CompactType>>, SurrealError>> + Send + 'static
    {
        let conn = self.conn.clone();
        let config = self.config.clone();
        let registered = worker.clone();
        let instance = self.instance.clone();
        let storage_name = self.storage_name;
        let register = stream::once(async move {
            crate::register_with_retry(&conn, &config, &registered, storage_name, &instance).await
        });
        let eager = SurrealPollFetcher::<CompactType, Decode>::new(
            &self.conn,
            &self.config,
            worker,
            self.instance.clone(),
        )
        .boxed();
        let lazy = self.fetcher.map(|task| Ok(Some(task))).boxed();
        register.chain(select(lazy, eager))
    }
}
