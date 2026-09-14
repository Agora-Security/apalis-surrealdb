use apalis_core::backend::{BackendExt, Vacuum};
use surrealdb::types::{Duration, Value};
use ulid::Ulid;

use crate::{CompactType, SurrealContext, SurrealError, SurrealStorage};

const VACUUM: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/queries/backend/vacuum.surql"
));

const DEFAULT_RETENTION: std::time::Duration = std::time::Duration::from_secs(3600);

impl<Args, D, F> Vacuum for SurrealStorage<Args, D, F>
where
    Self: BackendExt<
            Context = SurrealContext,
            Compact = CompactType,
            IdType = Ulid,
            Error = SurrealError,
        >,
{
    fn vacuum(&mut self) -> impl Future<Output = Result<usize, Self::Error>> + Send {
        let conn = self.conn.clone();
        let queue = self.config.queue().to_string();
        async move {
            let mut response = conn
                .query(VACUUM)
                .bind(("queue", queue))
                .bind(("retain", Duration::from_std(DEFAULT_RETENTION)))
                .await?;
            let deleted: Vec<Value> = response.take(0)?;
            Ok(deleted.len())
        }
    }
}
