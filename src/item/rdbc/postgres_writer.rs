use serde::Serialize;
use sqlx::{Pool, Postgres, QueryBuilder};

use crate::core::item::{ItemWriter, ItemWriterResult};
use crate::item::rdbc::ColumnValue;

use super::writer_common::{
    bind_column_value, create_write_error, log_write_success, max_items_per_batch, validate_config,
};

/// A writer for inserting items into a PostgreSQL database using SQLx.
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
/// use sqlx::PgPool;
/// use serde::Serialize;
///
/// #[derive(Clone, Serialize)]
/// struct User { id: i32, name: String }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let pool = PgPool::connect("postgresql://user:pass@localhost/db").await?;
///
/// let writer = RdbcItemWriterBuilder::<User>::new()
///     .postgres(&pool)
///     .table("users")
///     .column("id", |u: &User| u.id.into())
///     .column("name", |u: &User| u.name.as_str().into())
///     .build_postgres();
/// # Ok(())
/// # }
/// ```
pub struct PostgresItemWriter<O> {
    pub(crate) pool: Option<sqlx::Pool<Postgres>>,
    pub(crate) table: Option<String>,
    #[allow(clippy::type_complexity)]
    pub(crate) column_bindings: Vec<(String, Box<dyn Fn(&O) -> ColumnValue>)>,
    pub(crate) concurrency: usize,
}

impl<O> PostgresItemWriter<O> {
    /// Creates a new `PostgresItemWriter` with default configuration.
    pub(crate) fn new() -> Self {
        Self {
            pool: None,
            table: None,
            column_bindings: Vec::new(),
            concurrency: 1,
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
    ///     .build_postgres();
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
    pub(crate) fn pool(mut self, pool: &Pool<Postgres>) -> Self {
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

impl<O> Default for PostgresItemWriter<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Serialize + Clone> ItemWriter<O> for PostgresItemWriter<O> {
    fn write(&self, items: &[O]) -> ItemWriterResult {
        if items.is_empty() {
            return Ok(());
        }

        let (pool, table) = validate_config(
            self.pool.as_ref(),
            self.table.as_deref(),
            self.column_bindings.len(),
        )?;

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
                return Err(create_write_error(table, "PostgreSQL", e));
            }
        }

        log_write_success(items.len(), table, "PostgreSQL");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::item::rdbc::ColumnValue;

    #[test]
    fn should_start_with_empty_state() {
        let writer = PostgresItemWriter::<String>::new();
        assert!(writer.pool.is_none());
        assert!(writer.table.is_none());
        assert!(writer.column_bindings.is_empty());
    }

    #[test]
    fn should_store_column_bindings_in_order() {
        let writer = PostgresItemWriter::<String>::new()
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
        let writer = PostgresItemWriter::<String>::new();
        assert!(writer.write(&[]).is_ok());
    }

    #[test]
    fn should_return_error_when_no_columns_and_items_given() {
        use crate::BatchError;
        let writer = PostgresItemWriter::<String>::new().table("t");
        let result = writer.write(&["x".to_string()]);
        match result.err().unwrap() {
            BatchError::ItemWriter(msg) => assert!(msg.contains("columns"), "{msg}"),
            e => panic!("expected ItemWriter, got {e:?}"),
        }
    }

    #[test]
    fn should_return_error_when_pool_not_configured() {
        use crate::BatchError;
        let writer = PostgresItemWriter::<String>::new()
            .table("t")
            .add_column_binding("v".to_string(), Box::new(|s: &String| s.as_str().into()));
        let result = writer.write(&["x".to_string()]);
        match result.err().unwrap() {
            BatchError::ItemWriter(msg) => assert!(msg.contains("pool"), "{msg}"),
            e => panic!("expected ItemWriter, got {e:?}"),
        }
    }
}
