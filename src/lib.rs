#![doc = include_str!("../README.md")]

use std::sync::Arc;
use std::time::Duration;
use std::{fmt, marker::PhantomData};

use apalis_codec::json::JsonCodec;
use apalis_core::{
    backend::{Backend, BackendExt, TaskStream, codec::Codec, queue::Queue},
    features_table,
    layers::Stack,
    task::Task,
    timer::Delay,
    worker::{context::WorkerContext, ext::ack::AcknowledgeLayer},
};
use apalis_sql::context::SqlContext;
use futures::{
    Stream, StreamExt, TryStreamExt,
    stream::{self, BoxStream, select},
};
use ulid::Ulid;

pub use apalis_sql::config::Config;
pub use apalis_sql::ext::TaskBuilderExt;
pub use surrealdb::{
    Surreal,
    engine::any::{Any, connect},
};

pub use crate::ack::{LockTaskLayer, SurrealAck};
pub use crate::errors::SurrealError;
pub use crate::fetcher::{SurrealFetcher, SurrealLiveFetcher, SurrealPollFetcher};
pub use crate::shared::{SharedSurrealError, SharedSurrealStorage};
use surrealdb::types::RecordId;

use crate::{
    queries::{
        fetch_next::fetch_next,
        keep_alive::{initial_heartbeat, keep_alive_stream},
        kill_task::kill_task,
        reenqueue_orphaned::reenqueue_orphaned_stream,
        with_jitter,
    },
    sink::SurrealSink,
};

mod ack;
mod errors;
/// Fetchers that stream due tasks from SurrealDB
pub mod fetcher;
mod from_row;
/// Typed SurrealQL operations backing the storage
pub mod queries;
/// Shared storage multiplexing job types over one connection
pub mod shared;
/// Buffered sink that flushes queued tasks into SurrealDB
pub mod sink;

/// Decode a claimed task's args, parking it instead of failing the stream
async fn decode_or_kill<Args, Decode>(
    conn: &Arc<Surreal<Any>>,
    res: Result<Option<SurrealTask<CompactType>>, SurrealError>,
) -> Result<Option<SurrealTask<Args>>, SurrealError>
where
    Decode: Codec<Args, Compact = CompactType>,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    let Some(task) = res? else {
        return Ok(None);
    };
    let task_id = task.parts.task_id;
    match task.try_map(|t| Decode::decode(&t)) {
        Ok(task) => Ok(Some(task)),
        Err(e) => {
            log::error!("Killing task {task_id:?} with args this codec cannot decode: {e}");
            if let Some(id) = task_id {
                let id = RecordId::new(JOB_TABLE, id.to_string());
                if let Err(e) = kill_task(conn, &id, &e.to_string()).await {
                    log::error!("Failed to kill undecodable task: {e}");
                }
            }
            Ok(None)
        }
    }
}

const REGISTER_ATTEMPTS: u32 = 5;

const REGISTER_BACKOFF_BASE: Duration = Duration::from_millis(250);

pub(crate) async fn register_with_retry(
    conn: &Arc<Surreal<Any>>,
    config: &Config,
    worker: &WorkerContext,
    storage_name: &str,
    instance: &str,
) -> Result<Option<SurrealTask<CompactType>>, SurrealError> {
    for attempt in 0..REGISTER_ATTEMPTS {
        match initial_heartbeat(conn, config, worker, storage_name, instance).await {
            Ok(()) => return Ok(None),
            Err(e) => {
                log::warn!(
                    "Failed to register worker {} (instance {instance}) on attempt {} of \
                     {REGISTER_ATTEMPTS}: {e}",
                    worker.name(),
                    attempt + 1
                );
                if attempt + 1 < REGISTER_ATTEMPTS {
                    Delay::new(with_jitter(REGISTER_BACKOFF_BASE * 2u32.pow(attempt))).await;
                }
            }
        }
    }
    log::error!(
        "Giving up on registering worker {} (instance {instance}) after {REGISTER_ATTEMPTS} \
         attempts; it will keep polling and the keep-alive stream will register it once the \
         database is reachable",
        worker.name()
    );
    Ok(None)
}

pub(crate) fn new_instance() -> Arc<str> {
    Arc::from(Ulid::new().to_string())
}

const SCHEMA: &str = include_str!("schema.surql");

const SCHEMA_VERSION: i64 = 2;

pub const JOB_TABLE: &str = "job";

pub const WORKER_TABLE: &str = "worker";

/// The task context stored alongside every job in SurrealDB
pub type SurrealContext = SqlContext<Surreal<Any>>;

/// A task as stored and retrieved from the SurrealDB backend
pub type SurrealTask<Args> = Task<Args, SurrealContext, Ulid>;

/// The compact representation used when serializing task arguments to `bytes`
pub type CompactType = Vec<u8>;

/// A storage backend for apalis backed by SurrealDB
#[doc = features_table! {
    setup = r#"
        # {
        #   use apalis_surrealdb::{SurrealStorage, connect};
        #   let conn = std::sync::Arc::new(connect("mem://").await.unwrap());
        #   conn.use_ns("test").use_db("test").await.unwrap();
        #   SurrealStorage::setup(&conn).await.unwrap();
        #   SurrealStorage::new(&conn)
        # };
    "#,

    Backend => supported("Store and fetch tasks from SurrealDB", true),
    TaskSink => supported("Push new tasks onto a queue", true),
    Serialization => supported("Pluggable codec for task arguments", true),
    WaitForCompletion => supported("Await task completion without blocking", true),
    FetchById => supported("Fetch a task by its id", false),
    RegisterWorker => supported("Register and heartbeat workers with the backend", false),
    MakeShared => supported("Share one connection across queues via [`SharedSurrealStorage`](crate::SharedSurrealStorage)", false),
    ListWorkers => supported("List workers registered with the backend", false),
    ListTasks => supported("List and filter tasks in a queue", false),
    ListQueues => supported("List queues with their statistics", false),
    Metrics => supported("Collect queue and global statistics", false),
    Vacuum => supported("Purge completed tasks from storage", false),
}]
#[pin_project::pin_project]
pub struct SurrealStorage<T, C, Fetcher> {
    conn: Arc<Surreal<Any>>,
    /// Identifies this worker run
    instance: Arc<str>,
    storage_name: &'static str,
    job_type: PhantomData<T>,
    codec: PhantomData<C>,
    config: Config,
    #[pin]
    sink: SurrealSink<T, CompactType, C>,
    #[pin]
    fetcher: Fetcher,
}

impl<T, C, F> fmt::Debug for SurrealStorage<T, C, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SurrealStorage")
            .field("conn", &self.conn)
            .field("job_type", &"PhantomData<T>")
            .field("codec", &std::any::type_name::<C>())
            .field("config", &self.config)
            .field("storage_name", &self.storage_name)
            .finish()
    }
}

impl<T, C, F: Clone> Clone for SurrealStorage<T, C, F> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            instance: self.instance.clone(),
            storage_name: self.storage_name,
            job_type: PhantomData,
            codec: self.codec,
            config: self.config.clone(),
            sink: self.sink.clone(),
            fetcher: self.fetcher.clone(),
        }
    }
}

impl SurrealStorage<(), (), ()> {
    /// Define the tables, fields and indexes required by the backend.
    pub async fn setup(conn: &Arc<Surreal<Any>>) -> Result<(), SurrealError> {
        conn.query(SCHEMA).await?.check()?;
        conn.query("UPSERT apalis_meta:schema SET version = $version")
            .bind(("version", SCHEMA_VERSION))
            .await?
            .check()?;
        Ok(())
    }
}

impl<T> SurrealStorage<T, (), ()> {
    /// Create a new storage using the queue named after the task type
    #[must_use]
    pub fn new(
        conn: &Arc<Surreal<Any>>,
    ) -> SurrealStorage<T, JsonCodec<CompactType>, SurrealFetcher> {
        Self::new_with_config(conn, &Config::new(std::any::type_name::<T>()))
    }

    /// Create a new storage bound to a specific queue
    #[must_use]
    pub fn new_in_queue(
        conn: &Arc<Surreal<Any>>,
        queue: &str,
    ) -> SurrealStorage<T, JsonCodec<CompactType>, SurrealFetcher> {
        Self::new_with_config(conn, &Config::new(queue))
    }

    /// Create a new storage from an explicit [`Config`]
    #[must_use]
    pub fn new_with_config(
        conn: &Arc<Surreal<Any>>,
        config: &Config,
    ) -> SurrealStorage<T, JsonCodec<CompactType>, SurrealFetcher> {
        SurrealStorage {
            conn: conn.clone(),
            instance: new_instance(),
            storage_name: "SurrealStorage",
            job_type: PhantomData,
            codec: PhantomData,
            config: config.clone(),
            sink: SurrealSink::new(conn, config),
            fetcher: SurrealFetcher,
        }
    }

    /// Create a storage that reacts to live-query notifications, needs a ws or embedded connection
    #[must_use]
    pub fn new_with_events(
        conn: &Arc<Surreal<Any>>,
        config: &Config,
    ) -> SurrealStorage<T, JsonCodec<CompactType>, SurrealLiveFetcher> {
        SurrealStorage {
            conn: conn.clone(),
            instance: new_instance(),
            storage_name: "SurrealStorageWithEvents",
            job_type: PhantomData,
            codec: PhantomData,
            config: config.clone(),
            sink: SurrealSink::new(conn, config),
            fetcher: SurrealLiveFetcher::new(conn),
        }
    }
}

impl<T, C, F> SurrealStorage<T, C, F> {
    /// Change the codec used to serialize and deserialize task arguments
    pub fn with_codec<D>(self) -> SurrealStorage<T, D, F> {
        let sink = SurrealSink::new(&self.conn, &self.config);
        SurrealStorage {
            conn: self.conn,
            instance: self.instance,
            storage_name: self.storage_name,
            job_type: PhantomData,
            codec: PhantomData,
            config: self.config,
            sink,
            fetcher: self.fetcher,
        }
    }

    /// Get the config used by the storage
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Get the SurrealDB client used by the storage
    #[must_use]
    pub fn client(&self) -> &Surreal<Any> {
        &self.conn
    }
}

impl<Args, Decode, F> SurrealStorage<Args, Decode, F> {
    fn heartbeat_stream(
        &self,
        worker: &WorkerContext,
    ) -> BoxStream<'static, Result<(), SurrealError>> {
        let keep_alive = keep_alive_stream(
            self.conn.clone(),
            self.config.clone(),
            worker.clone(),
            self.storage_name,
            self.instance.clone(),
        );
        let orphan_interval = std::cmp::max(
            self.config.reenqueue_orphaned_after() / 2,
            *self.config.keep_alive(),
        );
        let reenqueue =
            reenqueue_orphaned_stream(self.conn.clone(), self.config.clone(), orphan_interval)
                .map_ok(|_| ());
        select(keep_alive, reenqueue).boxed()
    }

    fn middleware_stack(&self) -> Stack<LockTaskLayer, AcknowledgeLayer<SurrealAck>> {
        let lock = LockTaskLayer::new(self.conn.clone(), self.instance.clone());
        let ack = AcknowledgeLayer::new(SurrealAck::new(self.conn.clone()));
        Stack::new(lock, ack)
    }
}

impl<Args, Decode: Send + 'static, F> SurrealStorage<Args, Decode, F> {
    fn poll_default(
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
            register_with_retry(&conn, &config, &registered, storage_name, &instance).await
        });
        register.chain(SurrealPollFetcher::<CompactType, Decode>::new(
            &self.conn,
            &self.config,
            worker,
            self.instance.clone(),
        ))
    }
}

impl<Args, Decode: Send + 'static> SurrealStorage<Args, Decode, SurrealLiveFetcher> {
    fn poll_with_listener(
        self,
        worker: &WorkerContext,
    ) -> impl Stream<Item = Result<Option<SurrealTask<CompactType>>, SurrealError>> + Send + 'static
    {
        let worker = worker.clone();
        let reg_conn = self.conn.clone();
        let reg_config = self.config.clone();
        let reg_worker = worker.clone();
        let instance = self.instance.clone();
        let storage_name = self.storage_name;
        let register = stream::once(async move {
            register_with_retry(&reg_conn, &reg_config, &reg_worker, storage_name, &instance).await
        });

        let eager_fetcher: SurrealPollFetcher<CompactType, Decode> =
            SurrealPollFetcher::new(&self.conn, &self.config, &worker, self.instance.clone());

        let fetch_conn = self.conn.clone();
        let fetch_config = self.config.clone();
        let fetch_instance = self.instance.clone();
        let buffer_size = self.config.buffer_size();
        let mut consecutive_failures: u32 = 0;
        let lazy_fetcher = self
            .fetcher
            .ready_chunks(buffer_size)
            .then(move |_| {
                let conn = fetch_conn.clone();
                let config = fetch_config.clone();
                let instance = fetch_instance.clone();
                async move { fetch_next(&conn, &config, &instance).await }
            })
            .flat_map(move |res| match res {
                Ok(tasks) => {
                    consecutive_failures = 0;
                    stream::iter(tasks).map(Ok).boxed()
                }
                Err(e) => {
                    // the eager poller retries the same rows, so one failure is not an alarm
                    consecutive_failures = consecutive_failures.saturating_add(1);
                    log::log!(
                        fetcher::claim_failure_level(consecutive_failures),
                        failures = consecutive_failures;
                        "Failed to claim tasks woken by a live notification: {e}"
                    );
                    stream::empty().boxed()
                }
            })
            .map(|res| res.map(Some));

        register.chain(select(lazy_fetcher, eager_fetcher))
    }
}

impl<Args, Decode> Backend for SurrealStorage<Args, Decode, SurrealFetcher>
where
    Args: Send + Unpin + 'static,
    Decode: Codec<Args, Compact = CompactType> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;
    type IdType = Ulid;
    type Context = SurrealContext;
    type Error = SurrealError;
    type Stream = TaskStream<SurrealTask<Args>, SurrealError>;
    type Beat = BoxStream<'static, Result<(), SurrealError>>;
    type Layer = Stack<LockTaskLayer, AcknowledgeLayer<SurrealAck>>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        self.heartbeat_stream(worker)
    }

    fn middleware(&self) -> Self::Layer {
        self.middleware_stack()
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let conn = self.conn.clone();
        self.poll_default(worker)
            .then(move |a| {
                let conn = conn.clone();
                async move { decode_or_kill::<Args, Decode>(&conn, a).await }
            })
            .boxed()
    }
}

impl<Args, Decode: Send + 'static> BackendExt for SurrealStorage<Args, Decode, SurrealFetcher>
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
        self.poll_default(worker).boxed()
    }
}

impl<Args, Decode> Backend for SurrealStorage<Args, Decode, SurrealLiveFetcher>
where
    Args: Send + Unpin + 'static,
    Decode: Codec<Args, Compact = CompactType> + Send + 'static,
    Decode::Error: std::error::Error + Send + Sync + 'static,
{
    type Args = Args;
    type IdType = Ulid;
    type Context = SurrealContext;
    type Error = SurrealError;
    type Stream = TaskStream<SurrealTask<Args>, SurrealError>;
    type Beat = BoxStream<'static, Result<(), SurrealError>>;
    type Layer = Stack<LockTaskLayer, AcknowledgeLayer<SurrealAck>>;

    fn heartbeat(&self, worker: &WorkerContext) -> Self::Beat {
        self.heartbeat_stream(worker)
    }

    fn middleware(&self) -> Self::Layer {
        self.middleware_stack()
    }

    fn poll(self, worker: &WorkerContext) -> Self::Stream {
        let conn = self.conn.clone();
        self.poll_with_listener(worker)
            .then(move |a| {
                let conn = conn.clone();
                async move { decode_or_kill::<Args, Decode>(&conn, a).await }
            })
            .boxed()
    }
}

impl<Args, Decode: Send + 'static> BackendExt for SurrealStorage<Args, Decode, SurrealLiveFetcher>
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
        self.poll_with_listener(worker).boxed()
    }
}
