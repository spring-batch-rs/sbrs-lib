use std::cell::RefCell;

use serde::Serialize;
use sqlx::{MySql, Pool, QueryBuilder};

use crate::core::item::{ItemWriter, ItemWriterResult};
use crate::item::rdbc::ColumnValue;
use crate::item::rdbc::inflight_writes::InflightWrites;

use super::writer_common::{
    bind_column_value, create_write_error, log_write_success, max_items_per_batch, validate_config,
};

/// A writer for inserting items into a MySQL database using SQLx.
///
/// Supports batch INSERT via a list of column bindings supplied through
/// [`RdbcItemWriterBuilder::column`](crate::item::rdbc::RdbcItemWriterBuilder::column).
///
/// # Construction
///
/// Use [`RdbcItemWriterBuilder`](crate::item::rdbc::RdbcItemWriterBuilder) — direct
/// construction is not public.
///
/// # Examples
///
/// ```no_run
/// use spring_batch_rs::item::rdbc::{RdbcItemWriterBuilder, ColumnValue};
/// use sqlx::MySqlPool;
/// use serde::Serialize;
///
/// #[derive(Clone, Serialize)]
/// struct Product { id: i32, name: String, price: f64 }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let pool = MySqlPool::connect("mysql://user:pass@localhost/db").await?;
///
/// let writer = RdbcItemWriterBuilder::<Product>::new()
///     .mysql(&pool)
///     .table("products")
///     .column("id", |p: &Product| p.id.into())
///     .column("name", |p: &Product| p.name.as_str().into())
///     .column("price", |p: &Product| p.price.into())
///     .build_mysql();
/// # Ok(())
/// # }
/// ```
pub struct MySqlItemWriter<O> {
    pub(crate) pool: Option<sqlx::Pool<MySql>>,
    pub(crate) table: Option<String>,
    #[allow(clippy::type_complexity)]
    pub(crate) column_bindings: Vec<(String, Box<dyn Fn(&O) -> ColumnValue>)>,
    pub(crate) concurrency: usize,
    /// In-flight writes, created on first concurrent write.
    ///
    /// The `Option` is lazy initialisation: the table name is not known until
    /// `write` runs. Stays `None` for the sequential path.
    pub(crate) inflight: RefCell<Option<InflightWrites>>,
}

impl<O> MySqlItemWriter<O> {
    /// Creates a new `MySqlItemWriter` with default configuration.
    pub(crate) fn new() -> Self {
        Self {
            pool: None,
            table: None,
            column_bindings: Vec::new(),
            concurrency: 1,
            inflight: RefCell::new(None),
        }
    }

    /// Returns how many chunk writes this writer may keep in flight at once.
    ///
    /// `1` — the default — means writes are sequential. See
    /// [`RdbcItemWriterBuilder::with_concurrency`](crate::item::rdbc::RdbcItemWriterBuilder::with_concurrency).
    ///
    /// # Examples
    ///
    /// ```
    /// use spring_batch_rs::item::rdbc::RdbcItemWriterBuilder;
    ///
    /// let writer = RdbcItemWriterBuilder::<String>::new()
    ///     .table("t")
    ///     .column("v", |s: &String| s.as_str().into())
    ///     .build_mysql();
    ///
    /// assert_eq!(writer.concurrency(), 1, "writes are sequential unless opted in");
    /// ```
    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    /// Sets how many chunk writes may be in flight at once.
    pub(crate) fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Sets the database connection pool for the writer.
    pub(crate) fn pool(mut self, pool: &Pool<MySql>) -> Self {
        self.pool = Some(pool.clone());
        self
    }

    /// Sets the table name for the writer.
    pub(crate) fn table(mut self, table: &str) -> Self {
        self.table = Some(table.to_string());
        self
    }

    /// Adds a column binding to the writer.
    pub(crate) fn add_column_binding(
        mut self,
        name: String,
        extractor: Box<dyn Fn(&O) -> ColumnValue>,
    ) -> Self {
        self.column_bindings.push((name, extractor));
        self
    }
}

impl<O> Default for MySqlItemWriter<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O> MySqlItemWriter<O> {
    /// Writes the chunk with one blocking INSERT per bind-limited batch.
    ///
    /// This is the default path, taken whenever `concurrency <= 1`.
    fn write_sequential(&self, pool: &Pool<MySql>, table: &str, items: &[O]) -> ItemWriterResult {
        let col_names: Vec<&str> = self
            .column_bindings
            .iter()
            .map(|(n, _)| n.as_str())
            .collect();

        let col_list = col_names.join(",");
        let max_items = max_items_per_batch(self.column_bindings.len());

        for chunk in items.chunks(max_items) {
            let mut query_builder = QueryBuilder::new("INSERT INTO ");
            query_builder.push(table);
            query_builder.push(" (");
            query_builder.push(&col_list);
            query_builder.push(") ");

            query_builder.push_values(chunk.iter(), |mut b, item| {
                for (_, extractor) in &self.column_bindings {
                    bind_column_value!(b, extractor(item));
                }
            });

            let query = query_builder.build();
            let result = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async { query.execute(pool).await })
            });

            if let Err(e) = result {
                return Err(create_write_error(table, "MySQL", e));
            }
        }

        log_write_success(items.len(), table, "MySQL");
        Ok(())
    }

    /// Dispatches the chunk as a task, bounded by `concurrency`.
    ///
    /// Column values are extracted here, on the calling thread, because the
    /// extractor closures are not `Send`. Only the resulting `ColumnValue`s —
    /// which are `Send + 'static` — cross into the spawned task.
    ///
    /// # Errors
    ///
    /// Returns [`BatchError::ItemWriter`](crate::BatchError::ItemWriter) if an
    /// *earlier* write failed and was harvested while making room for this one.
    fn write_concurrent(&self, pool: &Pool<MySql>, table: &str, items: &[O]) -> ItemWriterResult {
        let col_list = self
            .column_bindings
            .iter()
            .map(|(n, _)| n.as_str())
            .collect::<Vec<_>>()
            .join(",");

        // Same bind-limit split as the sequential path: a chunk larger than the
        // limit must become several INSERTs on both paths, or a chunk size that
        // works sequentially would fail once concurrency is enabled.
        let max_items = max_items_per_batch(self.column_bindings.len());

        let mut guard = self.inflight.borrow_mut();
        let inflight = guard.get_or_insert_with(|| {
            InflightWrites::new(self.concurrency, table.to_string(), "MySQL")
        });

        let mut first_error = Ok(());
        for chunk in items.chunks(max_items) {
            let batch: Vec<Vec<ColumnValue>> = chunk
                .iter()
                .map(|item| {
                    self.column_bindings
                        .iter()
                        .map(|(_, extractor)| extractor(item))
                        .collect()
                })
                .collect();

            let pool = pool.clone();
            let table_owned = table.to_string();
            let col_list = col_list.clone();

            let dispatched = inflight.spawn(async move {
                let mut query_builder = QueryBuilder::new("INSERT INTO ");
                query_builder.push(&table_owned);
                query_builder.push(" (");
                query_builder.push(&col_list);
                query_builder.push(") ");
                // `into_iter` rather than `iter`: bind_column_value! moves the value
                // out of the enum, so consuming the rows avoids a clone per column
                // on the hot path.
                query_builder.push_values(batch, |mut b, row| {
                    for value in row {
                        bind_column_value!(b, value);
                    }
                });
                query_builder.build().execute(&pool).await.map(|_| ())
            });

            if dispatched.is_err() && first_error.is_ok() {
                first_error = dispatched;
            }
        }

        first_error
    }
}

impl<O: Serialize + Clone> ItemWriter<O> for MySqlItemWriter<O> {
    fn write(&self, items: &[O]) -> ItemWriterResult {
        if items.is_empty() {
            return Ok(());
        }

        let (pool, table) = validate_config(
            self.pool.as_ref(),
            self.table.as_deref(),
            self.column_bindings.len(),
        )?;

        if self.concurrency <= 1 {
            return self.write_sequential(pool, table, items);
        }

        self.write_concurrent(pool, table, items)
    }

    /// Collects results of finished writes without waiting.
    ///
    /// Deliberately non-blocking: the step engine calls this after every chunk,
    /// so waiting here would serialise the concurrent path back into sequence.
    /// Always `Ok` on the sequential path, which keeps nothing in flight.
    fn flush(&self) -> ItemWriterResult {
        match self.inflight.borrow_mut().as_mut() {
            Some(inflight) => inflight.harvest(),
            None => Ok(()),
        }
    }

    /// Waits for every in-flight write and reports the first failure.
    fn close(&self) -> ItemWriterResult {
        match self.inflight.borrow_mut().as_mut() {
            Some(inflight) => inflight.drain(),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::item::rdbc::ColumnValue;

    #[test]
    fn should_start_with_empty_state() {
        let writer = MySqlItemWriter::<String>::new();
        assert!(writer.pool.is_none());
        assert!(writer.table.is_none());
        assert!(writer.column_bindings.is_empty());
    }

    #[test]
    fn should_store_column_bindings_in_order() {
        let writer = MySqlItemWriter::<String>::new()
            .add_column_binding("x".to_string(), Box::new(|_| ColumnValue::Null))
            .add_column_binding("y".to_string(), Box::new(|_| ColumnValue::Null));
        let names: Vec<&str> = writer
            .column_bindings
            .iter()
            .map(|(n, _)| n.as_str())
            .collect();
        assert_eq!(names, vec!["x", "y"]);
    }

    #[test]
    fn should_return_ok_for_empty_items() {
        let writer = MySqlItemWriter::<String>::new();
        assert!(writer.write(&[]).is_ok());
    }

    #[test]
    fn should_return_error_when_no_columns_and_items_given() {
        use crate::BatchError;
        let writer = MySqlItemWriter::<String>::new().table("t");
        let result = writer.write(&["x".to_string()]);
        match result.err().unwrap() {
            BatchError::ItemWriter(msg) => assert!(msg.contains("columns"), "{msg}"),
            e => panic!("expected ItemWriter, got {e:?}"),
        }
    }

    #[test]
    fn should_return_error_when_pool_not_configured() {
        use crate::BatchError;
        let writer = MySqlItemWriter::<String>::new()
            .table("t")
            .add_column_binding("v".to_string(), Box::new(|s: &String| s.as_str().into()));
        let result = writer.write(&["x".to_string()]);
        match result.err().unwrap() {
            BatchError::ItemWriter(msg) => assert!(msg.contains("pool"), "{msg}"),
            e => panic!("expected ItemWriter, got {e:?}"),
        }
    }

    // --- concurrency ---

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_take_the_sequential_path_when_concurrency_is_one() {
        let writer = MySqlItemWriter::<String>::new();

        let result = writer.write(&["a".to_string()]);

        assert!(result.is_err(), "no pool means validate_config must reject");
        assert_eq!(writer.concurrency, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_report_no_error_from_flush_and_close_when_idle() {
        let writer = MySqlItemWriter::<String>::new();

        assert!(ItemWriter::<String>::flush(&writer).is_ok());
        assert!(ItemWriter::<String>::close(&writer).is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_delay_the_write_error_to_close_when_concurrent() {
        use crate::BatchError;

        // A lazy pool never connects until a query runs, so every dispatched
        // write fails — without needing a database to fail against.
        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(std::time::Duration::from_millis(200))
            .connect_lazy("mysql://nobody:nobody@127.0.0.1:1/nowhere")
            .expect("a lazy pool is built without connecting");

        let writer = MySqlItemWriter::<String>::new()
            .with_concurrency(2)
            .pool(&pool)
            .table("t")
            .add_column_binding("v".to_string(), Box::new(|s: &String| s.as_str().into()));

        assert!(
            writer.write(&["a".to_string()]).is_ok(),
            "a concurrent write is dispatched, not awaited, so it cannot fail here"
        );

        let closed = ItemWriter::<String>::close(&writer);
        assert!(
            closed.is_err(),
            "close() drains, so the failed write must surface there"
        );
        match closed.err().unwrap() {
            BatchError::ItemWriter(msg) => assert!(msg.contains("MySQL"), "{msg}"),
            e => panic!("expected ItemWriter, got {e:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_split_a_chunk_larger_than_the_bind_limit_when_concurrent() {
        use crate::item::rdbc::writer_common::BIND_LIMIT;

        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(std::time::Duration::from_millis(200))
            .connect_lazy("mysql://nobody:nobody@127.0.0.1:1/nowhere")
            .expect("a lazy pool is built without connecting");

        let writer = MySqlItemWriter::<String>::new()
            .with_concurrency(2)
            .pool(&pool)
            .table("t")
            .add_column_binding("v".to_string(), Box::new(|s: &String| s.as_str().into()));

        // One column, so the bind limit allows BIND_LIMIT rows per INSERT.
        // Exceeding it must produce several INSERTs, exactly as the sequential
        // path does, rather than one oversized statement.
        let items = vec!["x".to_string(); BIND_LIMIT + 10];

        assert!(
            writer.write(&items).is_ok(),
            "an oversized chunk must be split and dispatched, not rejected"
        );
        assert!(
            ItemWriter::<String>::close(&writer).is_err(),
            "the dead pool still fails every dispatched batch"
        );
    }
}
