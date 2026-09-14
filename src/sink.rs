use std::{
    fmt,
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures::{FutureExt, Sink};
use surrealdb::{Surreal, engine::any::Any};

use crate::{
    CompactType, Config, SurrealError, SurrealStorage, SurrealTask, queries::push_tasks::push_tasks,
};

type FlushFuture = Pin<
    Box<
        dyn Future<Output = Result<(), (SurrealError, Vec<SurrealTask<CompactType>>)>>
            + Send
            + Sync
            + 'static,
    >,
>;

/// Buffered sink that flushes queued tasks into the SurrealDB backend
#[pin_project::pin_project]
pub struct SurrealSink<Args, Compact, Codec> {
    conn: Arc<Surreal<Any>>,
    config: Config,
    buffer: Vec<SurrealTask<Compact>>,
    #[pin]
    flush_future: Option<FlushFuture>,
    _marker: PhantomData<(Args, Codec)>,
}

impl<Args, Compact, Codec> fmt::Debug for SurrealSink<Args, Compact, Codec> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SurrealSink")
            .field("config", &self.config)
            .field("buffered", &self.buffer.len())
            .field("flushing", &self.flush_future.is_some())
            .finish()
    }
}

impl<Args, Compact, Codec> SurrealSink<Args, Compact, Codec> {
    /// Create a sink that flushes tasks to the given queue
    #[must_use]
    pub fn new(conn: &Arc<Surreal<Any>>, config: &Config) -> Self {
        Self {
            conn: conn.clone(),
            config: config.clone(),
            buffer: Vec::new(),
            flush_future: None,
            _marker: PhantomData,
        }
    }
}

impl<Args, Compact, Codec> Clone for SurrealSink<Args, Compact, Codec> {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            config: self.config.clone(),
            buffer: Vec::new(),
            flush_future: None,
            _marker: PhantomData,
        }
    }
}

impl<Args, C, Fetcher> Sink<SurrealTask<CompactType>> for SurrealStorage<Args, C, Fetcher>
where
    Args: Send + Sync + 'static,
{
    type Error = SurrealError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        {
            let sink = self.as_mut().project().sink.get_mut();
            if sink.buffer.len() < sink.config.buffer_size().max(1) {
                return Poll::Ready(Ok(()));
            }
        }
        self.poll_flush(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: SurrealTask<CompactType>) -> Result<(), Self::Error> {
        self.project().sink.buffer.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let sink = self.project().sink.get_mut();

        loop {
            if let Some(flush) = sink.flush_future.as_mut() {
                match flush.poll_unpin(cx) {
                    Poll::Ready(Ok(())) => sink.flush_future = None,
                    Poll::Ready(Err((err, batch))) => {
                        sink.flush_future = None;
                        let batch_size = batch.len();
                        sink.buffer.splice(0..0, batch);
                        log::error!(
                            "Failed to flush a batch of {batch_size} task(s) to SurrealDB: {err}. The batch was returned to the buffer and will be retried on the next flush."
                        );
                        return Poll::Ready(Err(err));
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            if sink.buffer.is_empty() {
                return Poll::Ready(Ok(()));
            }

            let conn = sink.conn.clone();
            let config = sink.config.clone();
            let buffer = std::mem::take(&mut sink.buffer);
            sink.flush_future = Some(Box::pin(async move {
                match push_tasks(&conn, &config, &buffer).await {
                    Ok(()) => Ok(()),
                    Err(e) => Err((e, buffer)),
                }
            }));
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}
