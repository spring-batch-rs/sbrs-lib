use sqlx::{MySql, Pool, Postgres, Sqlite};

use super::column_value::ColumnValue;
use super::mysql_writer::MySqlItemWriter;
use super::postgres_writer::PostgresItemWriter;
use super::sqlite_writer::SqliteItemWriter;

/// Unified builder for creating RDBC item writers for any supported database type.
///
/// This builder provides a consistent, fluent API for constructing database writers
/// regardless of the underlying database (PostgreSQL, MySQL, or SQLite).
/// Users specify the connection pool, table name, and column mappings via the
/// `.column()` method, and call `build_postgres()`, `build_mysql()`, or
/// `build_sqlite()` to produce the appropriate writer.
///
/// # Type Parameters
///
/// * `O` - The item type to write
///
/// # Examples
///
/// ## PostgreSQL
/// ```no_run
/// use spring_batch_rs::item::rdbc::{RdbcItemWriterBuilder, ColumnValue};
/// use sqlx::PgPool;
/// use serde::Serialize;
///
/// #[derive(Clone, Serialize)]
/// struct User {
///     id: i32,
///     name: String,
/// }
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
///
/// ## MySQL
/// ```no_run
/// use spring_batch_rs::item::rdbc::{RdbcItemWriterBuilder, ColumnValue};
/// use sqlx::MySqlPool;
/// use serde::Serialize;
///
/// #[derive(Clone, Serialize)]
/// struct Product {
///     id: i32,
///     name: String,
///     price: f64,
/// }
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
///
/// ## SQLite
/// ```no_run
/// use spring_batch_rs::item::rdbc::{RdbcItemWriterBuilder, ColumnValue};
/// use sqlx::SqlitePool;
/// use serde::Serialize;
///
/// #[derive(Clone, Serialize)]
/// struct Task {
///     id: i32,
///     title: String,
/// }
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let pool = SqlitePool::connect("sqlite::memory:").await?;
///
/// let writer = RdbcItemWriterBuilder::<Task>::new()
///     .sqlite(&pool)
///     .table("tasks")
///     .column("id", |t: &Task| t.id.into())
///     .column("title", |t: &Task| t.title.as_str().into())
///     .build_sqlite();
/// # Ok(())
/// # }
/// ```
pub struct RdbcItemWriterBuilder<O> {
    postgres_pool: Option<sqlx::Pool<Postgres>>,
    mysql_pool: Option<sqlx::Pool<MySql>>,
    sqlite_pool: Option<sqlx::Pool<Sqlite>>,
    table: Option<String>,
    #[allow(clippy::type_complexity)]
    column_bindings: Vec<(String, Box<dyn Fn(&O) -> ColumnValue>)>,
    concurrency: usize,
}

impl<O> RdbcItemWriterBuilder<O> {
    /// Creates a new unified writer builder with default configuration.
    pub fn new() -> Self {
        Self {
            postgres_pool: None,
            mysql_pool: None,
            sqlite_pool: None,
            table: None,
            column_bindings: Vec::new(),
            concurrency: 1,
        }
    }

    /// Sets the PostgreSQL connection pool.
    ///
    /// # Arguments
    /// * `pool` - The PostgreSQL connection pool
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use spring_batch_rs::item::rdbc::RdbcItemWriterBuilder;
    /// use sqlx::PgPool;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = PgPool::connect("postgresql://user:pass@localhost/db").await?;
    /// let builder = RdbcItemWriterBuilder::<String>::new().postgres(&pool);
    /// # Ok(())
    /// # }
    /// ```
    pub fn postgres(mut self, pool: &Pool<Postgres>) -> Self {
        self.postgres_pool = Some(pool.clone());
        self
    }

    /// Sets the MySQL connection pool.
    ///
    /// # Arguments
    /// * `pool` - The MySQL connection pool
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use spring_batch_rs::item::rdbc::RdbcItemWriterBuilder;
    /// use sqlx::MySqlPool;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = MySqlPool::connect("mysql://user:pass@localhost/db").await?;
    /// let builder = RdbcItemWriterBuilder::<String>::new().mysql(&pool);
    /// # Ok(())
    /// # }
    /// ```
    pub fn mysql(mut self, pool: &Pool<MySql>) -> Self {
        self.mysql_pool = Some(pool.clone());
        self
    }

    /// Sets the SQLite connection pool.
    ///
    /// # Arguments
    /// * `pool` - The SQLite connection pool
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use spring_batch_rs::item::rdbc::RdbcItemWriterBuilder;
    /// use sqlx::SqlitePool;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = SqlitePool::connect("sqlite::memory:").await?;
    /// let builder = RdbcItemWriterBuilder::<String>::new().sqlite(&pool);
    /// # Ok(())
    /// # }
    /// ```
    pub fn sqlite(mut self, pool: &Pool<Sqlite>) -> Self {
        self.sqlite_pool = Some(pool.clone());
        self
    }

    /// Sets the table name for the writer.
    ///
    /// # Arguments
    /// * `table` - The database table name
    ///
    /// # Examples
    ///
    /// ```
    /// use spring_batch_rs::item::rdbc::RdbcItemWriterBuilder;
    ///
    /// let builder = RdbcItemWriterBuilder::<String>::new().table("users");
    /// ```
    pub fn table(mut self, table: &str) -> Self {
        self.table = Some(table.to_string());
        self
    }

    /// Sets how many chunk writes may be in flight at once.
    ///
    /// Defaults to `1`, which keeps the sequential behaviour: each `write` call
    /// completes its INSERT before returning. Values above `1` dispatch writes
    /// concurrently over the connection pool, which changes two observable things:
    ///
    /// - Chunks are no longer written in order.
    /// - A write error surfaces on a later `write` call, or at `close`, rather than
    ///   the call that dispatched it. `skip_limit` therefore applies with a delay of
    ///   up to `concurrency` chunks.
    ///
    /// Only PostgreSQL and MySQL honour this. SQLite serialises writes behind a
    /// database-level lock, so [`build_sqlite`](Self::build_sqlite) ignores the setting
    /// and logs a warning.
    ///
    /// Keep this at or below the connection pool size; extra tasks would only queue
    /// waiting for a connection.
    ///
    /// # Arguments
    /// * `concurrency` - Maximum number of chunk writes in flight. `0` is treated as `1`.
    ///
    /// # Examples
    ///
    /// ```
    /// use spring_batch_rs::item::rdbc::RdbcItemWriterBuilder;
    /// use spring_batch_rs::item::rdbc::ColumnValue;
    ///
    /// struct Order { id: i32 }
    ///
    /// let writer = RdbcItemWriterBuilder::<Order>::new()
    ///     .table("orders")
    ///     .column("id", |o: &Order| ColumnValue::Int(o.id as i64))
    ///     .with_concurrency(4)
    ///     .build_postgres();
    ///
    /// assert_eq!(writer.concurrency(), 4);
    /// ```
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Adds a column mapping for the writer.
    ///
    /// The `extractor` closure is called once per item per write, and must return
    /// a [`ColumnValue`] representing the value to bind for this column.
    ///
    /// Columns are inserted in the order they are added. Each call appends one
    /// column to the INSERT statement.
    ///
    /// # Arguments
    /// * `name` - The column name in the database table
    /// * `extractor` - Closure that extracts a [`ColumnValue`] from an item
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use spring_batch_rs::item::rdbc::{RdbcItemWriterBuilder, ColumnValue};
    ///
    /// struct User { id: i32, name: String }
    ///
    /// let builder = RdbcItemWriterBuilder::<User>::new()
    ///     .column("id", |u: &User| u.id.into())
    ///     .column("name", |u: &User| u.name.as_str().into());
    /// ```
    pub fn column(mut self, name: &str, extractor: impl Fn(&O) -> ColumnValue + 'static) -> Self {
        self.column_bindings
            .push((name.to_string(), Box::new(extractor)));
        self
    }

    /// Builds a PostgreSQL writer from the accumulated configuration.
    ///
    /// # Returns
    /// A configured [`PostgresItemWriter`] instance ready to use.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use spring_batch_rs::item::rdbc::RdbcItemWriterBuilder;
    /// use sqlx::PgPool;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = PgPool::connect("postgresql://user:pass@localhost/db").await?;
    /// let writer = RdbcItemWriterBuilder::<String>::new()
    ///     .postgres(&pool)
    ///     .table("t")
    ///     .column("v", |s: &String| s.as_str().into())
    ///     .build_postgres();
    /// # Ok(())
    /// # }
    /// ```
    pub fn build_postgres(self) -> PostgresItemWriter<O> {
        let mut writer = PostgresItemWriter::new().with_concurrency(self.concurrency);

        if let Some(pool) = self.postgres_pool {
            writer = writer.pool(&pool);
        }

        if let Some(table) = self.table {
            writer = writer.table(&table);
        }

        for (name, extractor) in self.column_bindings {
            writer = writer.add_column_binding(name, extractor);
        }

        writer
    }

    /// Builds a MySQL writer from the accumulated configuration.
    ///
    /// # Returns
    /// A configured [`MySqlItemWriter`] instance ready to use.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use spring_batch_rs::item::rdbc::RdbcItemWriterBuilder;
    /// use sqlx::MySqlPool;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = MySqlPool::connect("mysql://user:pass@localhost/db").await?;
    /// let writer = RdbcItemWriterBuilder::<String>::new()
    ///     .mysql(&pool)
    ///     .table("t")
    ///     .column("v", |s: &String| s.as_str().into())
    ///     .build_mysql();
    /// # Ok(())
    /// # }
    /// ```
    pub fn build_mysql(self) -> MySqlItemWriter<O> {
        let mut writer = MySqlItemWriter::new().with_concurrency(self.concurrency);

        if let Some(pool) = self.mysql_pool {
            writer = writer.pool(&pool);
        }

        if let Some(table) = self.table {
            writer = writer.table(&table);
        }

        for (name, extractor) in self.column_bindings {
            writer = writer.add_column_binding(name, extractor);
        }

        writer
    }

    /// Builds a SQLite writer from the accumulated configuration.
    ///
    /// # Returns
    /// A configured [`SqliteItemWriter`] instance ready to use.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use spring_batch_rs::item::rdbc::RdbcItemWriterBuilder;
    /// use sqlx::SqlitePool;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = SqlitePool::connect("sqlite::memory:").await?;
    /// let writer = RdbcItemWriterBuilder::<String>::new()
    ///     .sqlite(&pool)
    ///     .table("t")
    ///     .column("v", |s: &String| s.as_str().into())
    ///     .build_sqlite();
    /// # Ok(())
    /// # }
    /// ```
    pub fn build_sqlite(self) -> SqliteItemWriter<O> {
        if self.concurrency > 1 {
            log::warn!(
                "with_concurrency({}) ignored for SQLite: writes are serialised by a \
                 database-level lock, so concurrency would add no throughput",
                self.concurrency
            );
        }

        let mut writer = SqliteItemWriter::new();

        if let Some(pool) = self.sqlite_pool {
            writer = writer.pool(&pool);
        }

        if let Some(table) = self.table {
            writer = writer.table(&table);
        }

        for (name, extractor) in self.column_bindings {
            writer = writer.add_column_binding(name, extractor);
        }

        writer
    }
}

impl<O> Default for RdbcItemWriterBuilder<O> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::item::ItemWriter;
    use crate::item::rdbc::ColumnValue;

    // --- helpers ---

    struct User {
        id: i32,
        name: String,
    }

    // --- happy path ---

    #[test]
    fn should_accumulate_columns_in_order() {
        let writer = RdbcItemWriterBuilder::<User>::new()
            .column("id", |u: &User| u.id.into())
            .column("name", |u: &User| u.name.as_str().into())
            .build_postgres();
        let names: Vec<&str> = writer
            .column_bindings
            .iter()
            .map(|(n, _)| n.as_str())
            .collect();
        assert_eq!(
            names,
            vec!["id", "name"],
            "columns must be in insertion order"
        );
    }

    #[test]
    fn should_set_table_in_postgres_writer() {
        let writer = RdbcItemWriterBuilder::<String>::new()
            .table("users")
            .build_postgres();
        assert_eq!(
            writer.table.as_deref(),
            Some("users"),
            "table name must be transferred to postgres writer"
        );
    }

    #[test]
    fn should_set_table_in_mysql_writer() {
        let writer = RdbcItemWriterBuilder::<String>::new()
            .table("products")
            .build_mysql();
        assert_eq!(
            writer.table.as_deref(),
            Some("products"),
            "table name must be transferred to mysql writer"
        );
    }

    #[test]
    fn should_set_table_in_sqlite_writer() {
        use crate::BatchError;
        // No pool configured → validate_config will fail on "pool", not on table/columns.
        // Reaching the "pool" error proves that table and column were transferred correctly.
        let writer = RdbcItemWriterBuilder::<String>::new()
            .table("items")
            .column("sku", |s: &String| s.as_str().into())
            .build_sqlite();
        let result = writer.write(&["x".to_string()]);
        match result.err().unwrap() {
            BatchError::ItemWriter(msg) => assert!(
                msg.contains("pool"),
                "table and columns were set, so error should be about pool, got: {msg}"
            ),
            e => panic!("expected ItemWriter, got {e:?}"),
        }
    }

    #[test]
    fn should_build_via_default() {
        // Default must not panic and must produce an empty builder.
        let builder = RdbcItemWriterBuilder::<String>::default();
        let writer = builder.build_postgres();
        assert!(
            writer.table.is_none(),
            "default builder should have no table"
        );
        assert!(
            writer.column_bindings.is_empty(),
            "default builder should have no column bindings"
        );
    }

    #[test]
    fn should_transfer_columns_to_mysql_writer() {
        let writer = RdbcItemWriterBuilder::<String>::new()
            .column("a", |s: &String| s.as_str().into())
            .column("b", |_: &String| ColumnValue::Null)
            .build_mysql();
        let names: Vec<&str> = writer
            .column_bindings
            .iter()
            .map(|(n, _)| n.as_str())
            .collect();
        assert_eq!(
            names,
            vec!["a", "b"],
            "columns must reach mysql writer in order"
        );
    }

    #[test]
    fn should_transfer_columns_to_sqlite_writer() {
        let writer = RdbcItemWriterBuilder::<String>::new()
            .column("x", |_: &String| ColumnValue::Null)
            .build_sqlite();
        assert_eq!(
            writer.column_bindings.len(),
            1,
            "one column binding should be transferred to sqlite writer"
        );
        assert_eq!(writer.column_bindings[0].0, "x");
    }

    #[test]
    fn should_have_no_pool_by_default_in_postgres_writer() {
        let writer = RdbcItemWriterBuilder::<String>::new().build_postgres();
        assert!(writer.pool.is_none(), "pool should be None when not set");
    }

    #[test]
    fn should_have_no_pool_by_default_in_mysql_writer() {
        let writer = RdbcItemWriterBuilder::<String>::new().build_mysql();
        assert!(writer.pool.is_none(), "pool should be None when not set");
    }

    // --- async tests ---

    #[tokio::test(flavor = "multi_thread")]
    async fn should_transfer_pool_to_sqlite_writer() {
        use crate::BatchError;
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        let writer = RdbcItemWriterBuilder::<String>::new()
            .sqlite(&pool)
            .table("t")
            .column("v", |s: &String| s.as_str().into())
            .build_sqlite();
        let result = writer.write(&["x".to_string()]);
        match result.err().unwrap() {
            BatchError::ItemWriter(msg) => assert!(
                msg.contains("SQLite"),
                "pool transferred — should get SQLite DB error, got: {msg}"
            ),
            e => panic!("expected ItemWriter, got {e:?}"),
        }
    }

    // --- concurrency ---

    #[test]
    fn should_default_concurrency_to_one() {
        let writer = RdbcItemWriterBuilder::<User>::new()
            .table("items")
            .column("id", |u: &User| ColumnValue::Int(u.id as i64))
            .build_postgres();

        assert_eq!(
            writer.concurrency, 1,
            "default must be sequential so nobody opts in by accident"
        );
    }

    #[test]
    fn should_carry_configured_concurrency_to_the_writer() {
        let writer = RdbcItemWriterBuilder::<User>::new()
            .table("items")
            .column("id", |u: &User| ColumnValue::Int(u.id as i64))
            .with_concurrency(4)
            .build_postgres();

        assert_eq!(writer.concurrency, 4);
    }

    #[test]
    fn should_carry_configured_concurrency_to_the_mysql_writer() {
        let writer = RdbcItemWriterBuilder::<User>::new()
            .table("items")
            .column("id", |u: &User| ColumnValue::Int(u.id as i64))
            .with_concurrency(4)
            .build_mysql();

        assert_eq!(writer.concurrency, 4);
    }

    #[test]
    fn should_force_sqlite_to_stay_sequential() {
        let writer = RdbcItemWriterBuilder::<User>::new()
            .table("items")
            .column("id", |u: &User| ColumnValue::Int(u.id as i64))
            .with_concurrency(8)
            .build_sqlite();

        // SqliteItemWriter has no concurrency field at all — this test asserts the
        // builder compiles and yields a working writer, i.e. the setting is
        // accepted and ignored rather than rejected.
        assert!(writer.table.is_some());
    }
}
