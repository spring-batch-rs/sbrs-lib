/// Type-erased column value for item writers.
mod column_value;

/// Fluent SQL SELECT builder for RDBC item readers.
mod select_builder;

/// Common utilities for database item writers.
mod writer_common;

/// Bounded concurrency for in-flight chunk writes.
mod inflight_writes;

/// Common utilities for database item readers.
mod reader_common;

/// Database type enumeration.
mod database_type;

/// Unified reader builder for all database types.
mod unified_reader_builder;

/// Unified writer builder for all database types.
mod unified_writer_builder;

/// PostgreSQL-specific reader implementation.
pub mod postgres_reader;

/// MySQL-specific reader implementation.
pub mod mysql_reader;

/// SQLite-specific reader implementation.
pub mod sqlite_reader;

/// PostgreSQL-specific writer implementation.
pub mod postgres_writer;

/// MySQL-specific writer implementation.
pub mod mysql_writer;

/// SQLite-specific writer implementation.
pub mod sqlite_writer;

// Re-export database-specific reader and writer types (for direct usage)
pub use mysql_reader::MySqlRdbcItemReader;
pub use mysql_writer::MySqlItemWriter;
pub use postgres_reader::PostgresRdbcItemReader;
pub use postgres_writer::PostgresItemWriter;
pub use sqlite_reader::SqliteRdbcItemReader;
pub use sqlite_writer::SqliteItemWriter;

// Re-export unified builder types (recommended API)
pub use column_value::ColumnValue;
pub use database_type::DatabaseType;
pub use select_builder::SelectBuilder;
pub use unified_reader_builder::RdbcItemReaderBuilder;
pub use unified_writer_builder::RdbcItemWriterBuilder;
