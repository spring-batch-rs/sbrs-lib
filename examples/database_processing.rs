//! # Database Processing Examples
//!
//! Demonstrates reading from and writing to databases with Spring Batch RS.
//! Uses SQLite for all examples (no external database required).
//!
//! ## Features Demonstrated
//! - Reading from database with pagination
//! - Writing to database with batch inserts
//! - Fluent column binding API for type-safe binding
//! - Database to CSV/JSON export
//! - CSV import to database
//!
//! ## Run
//! ```bash
//! cargo run --example database_processing --features rdbc-sqlite,csv,json,logger
//! ```

use serde::{Deserialize, Serialize};
use spring_batch_rs::{
    BatchError,
    core::{
        item::ItemProcessor,
        job::{Job, JobBuilder},
        step::StepBuilder,
    },
    item::{
        csv::csv_reader::CsvItemReaderBuilder,
        csv::csv_writer::CsvItemWriterBuilder,
        json::json_writer::JsonItemWriterBuilder,
        logger::LoggerWriterBuilder,
        rdbc::{RdbcItemReaderBuilder, RdbcItemWriterBuilder},
    },
};
use sqlx::{FromRow, SqlitePool};
use std::env::temp_dir;

// =============================================================================
// Data Structures
// =============================================================================

/// A user record for database operations.
#[derive(Debug, Clone, Deserialize, Serialize, FromRow)]
struct User {
    id: i32,
    name: String,
    email: String,
    active: bool,
}

/// A product record for import/export operations.
#[derive(Debug, Clone, Deserialize, Serialize, FromRow)]
struct Product {
    id: i32,
    name: String,
    price: f64,
    stock: i32,
}

/// Processor that marks all users as active.
struct ActivateUserProcessor;

impl ItemProcessor<User, User> for ActivateUserProcessor {
    fn process(&self, item: User) -> Result<Option<User>, BatchError> {
        Ok(Some(User {
            id: item.id,
            name: item.name,
            email: item.email,
            active: true,
        }))
    }
}

// =============================================================================
// Database Setup
// =============================================================================

/// Creates and seeds the SQLite database with sample data.
async fn setup_database() -> Result<SqlitePool, sqlx::Error> {
    // Create in-memory database
    let pool = SqlitePool::connect("sqlite::memory:").await?;

    // Create users table
    sqlx::query(
        r#"
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            active BOOLEAN NOT NULL DEFAULT 0
        )
        "#,
    )
    .execute(&pool)
    .await?;

    // Create products table
    sqlx::query(
        r#"
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL NOT NULL,
            stock INTEGER NOT NULL
        )
        "#,
    )
    .execute(&pool)
    .await?;

    // Seed users table
    let users = [
        (1, "Alice Johnson", "alice@example.com", true),
        (2, "Bob Smith", "bob@example.com", false),
        (3, "Charlie Brown", "charlie@example.com", true),
        (4, "Diana Prince", "diana@example.com", false),
        (5, "Eve Wilson", "eve@example.com", true),
    ];

    for (id, name, email, active) in users {
        sqlx::query("INSERT INTO users (id, name, email, active) VALUES (?, ?, ?, ?)")
            .bind(id)
            .bind(name)
            .bind(email)
            .bind(active)
            .execute(&pool)
            .await?;
    }

    Ok(pool)
}

// =============================================================================
// Example 1: Read from Database
// =============================================================================

/// Reads users from the database and logs them.
fn example_read_from_database(pool: &SqlitePool) -> Result<(), BatchError> {
    println!("=== Example 1: Read from Database ===");

    let reader = RdbcItemReaderBuilder::<User>::new()
        .sqlite(pool.clone())
        .query("SELECT id, name, email, active FROM users")
        .with_page_size(2)
        .build_sqlite();

    let writer = LoggerWriterBuilder::<User>::new().build();

    let step = StepBuilder::new("read-users")
        .chunk::<User, User>(2)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run()?;

    let step_exec = job.get_step_execution("read-users").unwrap();
    println!("  Users read: {}", step_exec.read_count);
    println!("  Duration: {:?}", result.duration);
    Ok(())
}

// =============================================================================
// Example 2: Database to JSON Export
// =============================================================================

/// Exports database records to a JSON file.
fn example_database_to_json(pool: &SqlitePool) -> Result<(), BatchError> {
    println!("\n=== Example 2: Database to JSON Export ===");

    let reader = RdbcItemReaderBuilder::<User>::new()
        .sqlite(pool.clone())
        .query("SELECT id, name, email, active FROM users WHERE active = 1")
        .build_sqlite();

    let output_path = temp_dir().join("active_users.json");
    let writer = JsonItemWriterBuilder::<User>::new()
        .pretty_formatter(true)
        .from_path(&output_path);

    let step = StepBuilder::new("export-to-json")
        .chunk::<User, User>(10)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    job.run()?;

    println!("  Exported active users to JSON");
    println!("  Output: {}", output_path.display());
    Ok(())
}

// =============================================================================
// Example 3: Database to CSV Export
// =============================================================================

/// Exports database records to a CSV file.
fn example_database_to_csv(pool: &SqlitePool) -> Result<(), BatchError> {
    println!("\n=== Example 3: Database to CSV Export ===");

    let reader = RdbcItemReaderBuilder::<User>::new()
        .sqlite(pool.clone())
        .query("SELECT id, name, email, active FROM users ORDER BY name")
        .build_sqlite();

    let output_path = temp_dir().join("users_export.csv");
    let writer = CsvItemWriterBuilder::<User>::new()
        .has_headers(true)
        .from_path(&output_path);

    let step = StepBuilder::new("export-to-csv")
        .chunk::<User, User>(10)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    job.run()?;

    println!("  Exported all users to CSV");
    println!("  Output: {}", output_path.display());
    Ok(())
}

// =============================================================================
// Example 4: CSV Import to Database
// =============================================================================

/// Imports products from CSV into the database.
fn example_csv_to_database(pool: &SqlitePool) -> Result<(), BatchError> {
    println!("\n=== Example 4: CSV Import to Database ===");

    let csv_data = "\
id,name,price,stock
1,Laptop,999.99,50
2,Mouse,29.99,200
3,Keyboard,79.99,150
4,Monitor,299.99,75
5,Headphones,149.99,100";

    let reader = CsvItemReaderBuilder::<Product>::new()
        .has_headers(true)
        .from_reader(csv_data.as_bytes());

    let writer = RdbcItemWriterBuilder::<Product>::new()
        .sqlite(pool)
        .table("products")
        .column("id", |p: &Product| p.id.into())
        .column("name", |p: &Product| p.name.clone().into())
        .column("price", |p: &Product| p.price.into())
        .column("stock", |p: &Product| p.stock.into())
        .build_sqlite();

    let step = StepBuilder::new("import-products")
        .chunk::<Product, Product>(2)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run()?;

    let step_exec = job.get_step_execution("import-products").unwrap();
    println!("  Products imported: {}", step_exec.write_count);
    println!("  Duration: {:?}", result.duration);
    Ok(())
}

// =============================================================================
// Example 5: Read and Transform Database Records
// =============================================================================

/// Reads users, transforms them (activates all), and writes back.
fn example_transform_and_write(pool: &SqlitePool) -> Result<(), BatchError> {
    println!("\n=== Example 5: Read, Transform, and Write ===");

    // Create a separate table for transformed users
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current().block_on(async {
            sqlx::query(
                r#"
                CREATE TABLE IF NOT EXISTS activated_users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT NOT NULL,
                    active BOOLEAN NOT NULL
                )
                "#,
            )
            .execute(pool)
            .await
        })
    })
    .map_err(|e| BatchError::ItemWriter(e.to_string()))?;

    let reader = RdbcItemReaderBuilder::<User>::new()
        .sqlite(pool.clone())
        .query("SELECT id, name, email, active FROM users WHERE active = 0")
        .build_sqlite();

    let writer = RdbcItemWriterBuilder::<User>::new()
        .sqlite(pool)
        .table("activated_users")
        .column("id", |u: &User| u.id.into())
        .column("name", |u: &User| u.name.clone().into())
        .column("email", |u: &User| u.email.clone().into())
        .column("active", |u: &User| u.active.into())
        .build_sqlite();

    let processor = ActivateUserProcessor;

    let step = StepBuilder::new("activate-users")
        .chunk::<User, User>(10)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run()?;

    let step_exec = job.get_step_execution("activate-users").unwrap();
    println!("  Inactive users activated: {}", step_exec.write_count);
    println!("  Duration: {:?}", result.duration);
    Ok(())
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), BatchError> {
    env_logger::init();

    println!("Database Processing Examples (SQLite)");
    println!("=====================================\n");

    // Setup database
    let pool = setup_database()
        .await
        .map_err(|e| BatchError::ItemReader(format!("Failed to setup database: {}", e)))?;

    println!("Database initialized with sample data.\n");

    // Run examples
    example_read_from_database(&pool)?;
    example_database_to_json(&pool)?;
    example_database_to_csv(&pool)?;
    example_csv_to_database(&pool)?;
    example_transform_and_write(&pool)?;

    println!("\n✓ All database examples completed successfully!");
    Ok(())
}
