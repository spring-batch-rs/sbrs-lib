//! Common functionality for database item writers.
//!
//! Provides shared utilities used across PostgreSQL, MySQL, and SQLite writers.

use crate::BatchError;
use sqlx::{Database, Pool};

/// The maximum number of parameters bound in a single SQL statement.
/// This is the most conservative limit across major databases (MySQL's limit).
pub(super) const BIND_LIMIT: usize = 65535;

/// Type alias for the validated configuration returned by [`validate_config`].
type ValidatedConfig<'a, DB> = (&'a Pool<DB>, &'a str);

/// Validates that all required writer configuration fields are set.
///
/// # Errors
///
/// Returns [`BatchError::ItemWriter`] if columns is zero, pool is `None`, or table is `None`.
pub(super) fn validate_config<'a, DB: Database>(
    pool: Option<&'a Pool<DB>>,
    table: Option<&'a str>,
    column_count: usize,
) -> Result<ValidatedConfig<'a, DB>, BatchError> {
    if column_count == 0 {
        return Err(BatchError::ItemWriter(
            "No columns specified for database write".to_string(),
        ));
    }

    let pool =
        pool.ok_or_else(|| BatchError::ItemWriter("Database pool not configured".to_string()))?;

    let table =
        table.ok_or_else(|| BatchError::ItemWriter("Table name not configured".to_string()))?;

    Ok((pool, table))
}

/// Logs a successful write operation.
///
/// # Arguments
///
/// * `items_count` - The number of items written
/// * `table` - The table name
/// * `db_name` - The database name (e.g., "PostgreSQL", "MySQL", "SQLite")
#[inline]
pub(super) fn log_write_success(items_count: usize, table: &str, db_name: &str) {
    log::debug!(
        "Successfully wrote {} items to {} table {}",
        items_count,
        db_name,
        table
    );
}

/// Creates a database write error.
///
/// # Arguments
///
/// * `table` - The table name
/// * `db_name` - The database name
/// * `error` - The underlying error
///
/// # Returns
///
/// A [`BatchError::ItemWriter`] with a formatted error message.
pub(super) fn create_write_error(
    table: &str,
    db_name: &str,
    error: impl std::fmt::Display,
) -> BatchError {
    log::error!(
        "Failed to write items to {} table {}: {}",
        db_name,
        table,
        error
    );
    BatchError::ItemWriter(format!("{} write failed: {}", db_name, error))
}

/// Calculates the maximum number of items per batch given the bind limit.
///
/// # Arguments
///
/// * `column_count` - The number of columns in the table
///
/// # Returns
///
/// `BIND_LIMIT / column_count`
#[inline]
pub(super) fn max_items_per_batch(column_count: usize) -> usize {
    BIND_LIMIT / column_count
}

/// Binds a [`ColumnValue`] to a `QueryBuilder::Separated` push slot.
///
/// Defined as a macro to work across `Sqlite`, `Postgres`, and `MySql`
/// `QueryBuilder` types without requiring database-generic trait bounds.
macro_rules! bind_column_value {
    ($b:expr, $val:expr) => {
        match $val {
            $crate::item::rdbc::ColumnValue::Int(v) => {
                $b.push_bind(v);
            }
            $crate::item::rdbc::ColumnValue::Float(v) => {
                $b.push_bind(v);
            }
            $crate::item::rdbc::ColumnValue::Text(v) => {
                $b.push_bind(v);
            }
            $crate::item::rdbc::ColumnValue::Bool(v) => {
                $b.push_bind(v);
            }
            $crate::item::rdbc::ColumnValue::Bytes(v) => {
                $b.push_bind(v);
            }
            $crate::item::rdbc::ColumnValue::Null => {
                $b.push_bind(Option::<String>::None);
            }
        }
    };
}
pub(crate) use bind_column_value;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BatchError;
    use sqlx::Sqlite;

    #[test]
    fn should_compute_max_items_per_batch() {
        assert_eq!(max_items_per_batch(1), 65535);
        assert_eq!(max_items_per_batch(2), 32767);
        assert_eq!(max_items_per_batch(10), 6553);
        assert_eq!(max_items_per_batch(100), 655);
    }

    #[test]
    fn should_define_bind_limit_as_65535() {
        assert_eq!(BIND_LIMIT, 65535);
    }

    #[test]
    fn should_return_error_when_columns_is_empty() {
        let result = validate_config::<Sqlite>(None, Some("tbl"), 0);
        match result.err().unwrap() {
            BatchError::ItemWriter(msg) => assert!(msg.contains("columns"), "unexpected: {msg}"),
            e => panic!("expected ItemWriter, got {e:?}"),
        }
    }

    #[test]
    fn should_return_error_when_pool_is_missing() {
        let result = validate_config::<Sqlite>(None, Some("tbl"), 1);
        match result.err().unwrap() {
            BatchError::ItemWriter(msg) => assert!(msg.contains("pool"), "unexpected: {msg}"),
            e => panic!("expected ItemWriter, got {e:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn should_return_error_when_table_is_missing() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        let result = validate_config::<Sqlite>(Some(&pool), None, 1);
        match result.err().unwrap() {
            BatchError::ItemWriter(msg) => assert!(msg.contains("Table"), "unexpected: {msg}"),
            e => panic!("expected ItemWriter, got {e:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn should_return_ok_when_all_config_provided() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        let result = validate_config::<Sqlite>(Some(&pool), Some("tbl"), 1);
        assert!(
            result.is_ok(),
            "should return Ok when all config is provided"
        );
    }

    #[test]
    fn should_call_log_write_success_without_panic() {
        log_write_success(42, "users", "PostgreSQL");
    }

    #[test]
    fn should_create_write_error_with_formatted_message() {
        let err = create_write_error("orders", "MySQL", "connection refused");
        match err {
            BatchError::ItemWriter(msg) => {
                assert!(msg.contains("MySQL"), "missing db name: {msg}");
                assert!(msg.contains("connection refused"), "missing cause: {msg}");
            }
            e => panic!("expected ItemWriter, got {e:?}"),
        }
    }
}
