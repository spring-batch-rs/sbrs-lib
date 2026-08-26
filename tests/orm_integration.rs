use std::cell::Cell;
use std::io::Read;
use std::str::FromStr;

use anyhow::Error;
use sea_orm::prelude::Decimal;
use sea_orm::{
    Database, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder, entity::prelude::*,
};
use serde::{Deserialize, Serialize};
use spring_batch_rs::{
    BatchError,
    core::{
        item::{ItemProcessor, ItemProcessorResult, ItemReader, ItemWriter},
        job::{Job, JobBuilder},
        step::{StepBuilder, StepStatus},
    },
    item::{
        csv::csv_writer::CsvItemWriterBuilder,
        orm::{OrmItemReaderBuilder, OrmItemWriterBuilder},
    },
};
use tempfile::NamedTempFile;
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

/// Test entity representing a Product in the database
#[derive(Debug, Clone, DeriveEntityModel, Deserialize, Serialize, PartialEq)]
#[sea_orm(table_name = "products")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub name: String,
    pub category: String,
    pub price: Decimal,
    pub in_stock: bool,
    pub created_at: DateTimeUtc,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

/// DTO for processed product data (for custom processing scenarios)
#[derive(Debug, Clone, Serialize)]
pub struct ProductDto {
    pub id: i32,
    pub display_name: String,
    pub category: String,
    pub formatted_price: String,
    pub availability: String,
}

/// Processor that converts Model to ProductDto for transformation
#[derive(Default)]
struct ProductTransformProcessor;

impl ItemProcessor<Model, ProductDto> for ProductTransformProcessor {
    fn process(&self, item: Model) -> ItemProcessorResult<ProductDto> {
        let dto = ProductDto {
            id: item.id,
            display_name: format!("Product: {}", item.name),
            category: item.category,
            formatted_price: format!("${:.2}", item.price),
            availability: if item.in_stock {
                "Available".to_string()
            } else {
                "Out of Stock".to_string()
            },
        };
        Ok(Some(dto))
    }
}

/// Sets up a test database with sample product data using SQLite
async fn setup_test_database() -> Result<DatabaseConnection, Error> {
    // Connect to an in-memory SQLite database
    let database_url = "sqlite::memory:";

    // Add connection options to prevent timeouts
    let mut connect_options = sea_orm::ConnectOptions::new(database_url);
    connect_options
        .max_connections(1)
        .min_connections(1)
        .connect_timeout(std::time::Duration::from_secs(10))
        .acquire_timeout(std::time::Duration::from_secs(10))
        .idle_timeout(std::time::Duration::from_secs(300))
        .max_lifetime(std::time::Duration::from_secs(1800));

    let db = Database::connect(connect_options).await?;

    // Create the products table
    let create_table_sql = r#"
        CREATE TABLE products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL,
            in_stock BOOLEAN NOT NULL DEFAULT 1,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    "#;

    db.execute_unprepared(create_table_sql).await?;

    // Insert sample data
    let insert_data_sql = r#"
        INSERT INTO products (name, category, price, in_stock, created_at) VALUES
        ('Laptop Pro 15', 'Electronics', 1299.99, 1, '2024-01-01 10:00:00'),
        ('Wireless Mouse', 'Electronics', 29.99, 1, '2024-01-02 11:00:00'),
        ('Office Chair', 'Furniture', 199.99, 0, '2024-01-03 12:00:00'),
        ('Standing Desk', 'Furniture', 399.99, 1, '2024-01-04 13:00:00'),
        ('Coffee Mug', 'Kitchen', 12.99, 1, '2024-01-05 14:00:00'),
        ('Bluetooth Speaker', 'Electronics', 79.99, 0, '2024-01-06 15:00:00'),
        ('Notebook Set', 'Office', 15.99, 1, '2024-01-07 16:00:00'),
        ('Desk Lamp', 'Furniture', 45.99, 1, '2024-01-08 17:00:00'),
        ('Keyboard Mechanical', 'Electronics', 129.99, 1, '2024-01-09 18:00:00'),
        ('Water Bottle', 'Kitchen', 19.99, 0, '2024-01-10 19:00:00'),
        ('Monitor 27inch', 'Electronics', 299.99, 1, '2024-01-11 20:00:00'),
        ('Pen Set', 'Office', 8.99, 1, '2024-01-12 21:00:00')
    "#;

    db.execute_unprepared(insert_data_sql).await?;

    Ok(db)
}

#[tokio::test(flavor = "multi_thread")]
async fn test_orm_reader_without_pagination() -> Result<(), Error> {
    let db = setup_test_database().await?;

    // Create a query to select all products
    let query = Entity::find().order_by_asc(Column::Id);

    // Create the reader without pagination
    let reader = OrmItemReaderBuilder::new()
        .connection(&db)
        .query(query)
        .build();

    // Prepare writer
    let tmpfile = NamedTempFile::new()?;
    let writer = CsvItemWriterBuilder::new()
        .has_headers(true)
        .from_writer(tmpfile.as_file());

    // Execute process
    let step = StepBuilder::new("test_orm_no_pagination")
        .chunk::<Model, Model>(5)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run();

    if let Err(ref e) = result {
        eprintln!("Job failed with error: {:?}", e);
        // Also check step execution for more details
        if let Some(step_execution) = job.get_step_execution("test_orm_no_pagination") {
            eprintln!("Step execution status: {:?}", step_execution.status);
            eprintln!("Step execution read_count: {}", step_execution.read_count);
            eprintln!(
                "Step execution read_error_count: {}",
                step_execution.read_error_count
            );
            eprintln!(
                "Step execution write_error_count: {}",
                step_execution.write_error_count
            );
        }
    }
    assert!(result.is_ok());

    let step_execution = job.get_step_execution("test_orm_no_pagination").unwrap();

    assert_eq!(step_execution.status, StepStatus::Success);
    assert_eq!(step_execution.read_count, 12);
    assert_eq!(step_execution.write_count, 12);
    assert_eq!(step_execution.process_count, 12);
    assert_eq!(step_execution.read_error_count, 0);
    assert_eq!(step_execution.write_error_count, 0);

    // Verify the content
    let mut tmpfile = tmpfile.reopen()?;
    let mut file_content = String::new();
    tmpfile.read_to_string(&mut file_content)?;

    // Check that we have the expected number of lines (header + 12 products)
    let lines: Vec<&str> = file_content.lines().collect();
    assert_eq!(lines.len(), 13); // header + 12 products

    // Check header
    assert!(lines[0].contains("id,name,category,price,in_stock,created_at"));

    // Check first product
    assert!(lines[1].contains("1,Laptop Pro 15,Electronics,1299.99,true"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_orm_reader_with_pagination() -> Result<(), Error> {
    let db = setup_test_database().await?;

    // Create a query to select products with pagination
    let query = Entity::find()
        .filter(Column::InStock.eq(true))
        .order_by_asc(Column::Id);

    // Create the reader with pagination (page size of 3)
    let reader = OrmItemReaderBuilder::new()
        .connection(&db)
        .query(query)
        .page_size(3)
        .build();

    // Prepare writer
    let tmpfile = NamedTempFile::new()?;
    let writer = CsvItemWriterBuilder::new()
        .has_headers(true)
        .from_writer(tmpfile.as_file());

    // Use transform processor to convert Model to ProductDto
    let processor = ProductTransformProcessor;

    // Execute process
    let step = StepBuilder::new("test_orm_pagination")
        .chunk::<Model, ProductDto>(4)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run();

    if let Err(ref e) = result {
        eprintln!("Job failed with error: {:?}", e);
    }
    assert!(result.is_ok());

    let step_execution = job.get_step_execution("test_orm_pagination").unwrap();

    assert_eq!(step_execution.status, StepStatus::Success);
    // Should read 9 products (only in_stock = true)
    assert_eq!(step_execution.read_count, 9);
    assert_eq!(step_execution.write_count, 9);
    assert_eq!(step_execution.process_count, 9);
    assert_eq!(step_execution.read_error_count, 0);
    assert_eq!(step_execution.write_error_count, 0);

    // Verify the content
    let mut tmpfile = tmpfile.reopen()?;
    let mut file_content = String::new();
    tmpfile.read_to_string(&mut file_content)?;

    // Check that we have the expected number of lines (header + 9 in-stock products)
    let lines: Vec<&str> = file_content.lines().collect();
    assert_eq!(lines.len(), 10); // header + 9 products

    // Check header
    assert!(lines[0].contains("id,display_name,category,formatted_price,availability"));

    // Check first product (should be transformed)
    assert!(
        lines[1].contains("1")
            && lines[1].contains("Product: Laptop Pro 15")
            && lines[1].contains("Electronics")
            && lines[1].contains("$1299.99")
            && lines[1].contains("Available")
    );

    // Verify all products are marked as available
    for line in &lines[1..] {
        assert!(line.contains("Available"));
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_orm_reader_with_complex_filtering() -> Result<(), Error> {
    let db = setup_test_database().await?;

    // Create a complex query: Electronics products under $100
    let query = Entity::find()
        .filter(Column::Category.eq("Electronics"))
        .filter(Column::Price.lt(100.0))
        .order_by_asc(Column::Price);

    // Create the reader
    let reader = OrmItemReaderBuilder::new()
        .connection(&db)
        .query(query)
        .page_size(2)
        .build();

    // Read items manually to test the reader directly
    let mut products = Vec::new();
    loop {
        match reader.read() {
            Ok(Some(product)) => products.push(product),
            Ok(None) => break,
            Err(e) => {
                eprintln!("Error reading product: {:?}", e);
                return Err(e.into());
            }
        }
    }

    // Should find 2 electronics products under $100: Wireless Mouse ($29.99) and Bluetooth Speaker ($79.99)
    assert_eq!(products.len(), 2);

    // Check first product (cheapest)
    assert_eq!(products[0].name, "Wireless Mouse");
    assert_eq!(products[0].price, Decimal::from_str("29.99")?);
    assert_eq!(products[0].category, "Electronics");

    // Check second product
    assert_eq!(products[1].name, "Bluetooth Speaker");
    assert_eq!(products[1].price, Decimal::from_str("79.99")?);
    assert_eq!(products[1].category, "Electronics");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_orm_reader_empty_result_set() -> Result<(), Error> {
    let db = setup_test_database().await?;

    // Create a query that returns no results
    let query = Entity::find()
        .filter(Column::Category.eq("NonExistentCategory"))
        .order_by_asc(Column::Id);

    // Create the reader
    let reader = OrmItemReaderBuilder::new()
        .connection(&db)
        .query(query)
        .page_size(5)
        .build();

    // Read items manually
    let mut count = 0;
    loop {
        match reader.read() {
            Ok(Some(_product)) => count += 1,
            Ok(None) => break,
            Err(e) => {
                eprintln!("Error reading product: {:?}", e);
                return Err(e.into());
            }
        }
    }

    // Should read 0 items
    assert_eq!(count, 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_orm_reader_single_item() -> Result<(), Error> {
    let db = setup_test_database().await?;

    // Create a query that returns exactly one result
    let query = Entity::find()
        .filter(Column::Id.eq(1))
        .order_by_asc(Column::Id);

    // Create the reader with pagination
    let reader = OrmItemReaderBuilder::new()
        .connection(&db)
        .query(query)
        .page_size(10)
        .build();

    // Read items manually
    let mut products = Vec::new();
    loop {
        match reader.read() {
            Ok(Some(product)) => products.push(product),
            Ok(None) => break,
            Err(e) => {
                eprintln!("Error reading product: {:?}", e);
                return Err(e.into());
            }
        }
    }

    // Should read exactly 1 item
    assert_eq!(products.len(), 1);
    assert_eq!(products[0].id, 1);
    assert_eq!(products[0].name, "Laptop Pro 15");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_orm_reader_large_page_size() -> Result<(), Error> {
    let db = setup_test_database().await?;

    // Create a query to select all products
    let query = Entity::find().order_by_asc(Column::Id);

    // Create the reader with a large page size (larger than total records)
    let reader = OrmItemReaderBuilder::new()
        .connection(&db)
        .query(query)
        .page_size(100)
        .build();

    // Read items manually
    let mut count = 0;
    loop {
        match reader.read() {
            Ok(Some(_product)) => count += 1,
            Ok(None) => break,
            Err(e) => {
                eprintln!("Error reading product: {:?}", e);
                return Err(e.into());
            }
        }
    }

    // Should read all 12 items
    assert_eq!(count, 12);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_orm_reader_integration_with_job() -> Result<(), Error> {
    let db = setup_test_database().await?;

    // Create a query for furniture products
    let query = Entity::find()
        .filter(Column::Category.eq("Furniture"))
        .order_by_desc(Column::Price);

    // Create the reader
    let reader = OrmItemReaderBuilder::new()
        .connection(&db)
        .query(query)
        .page_size(2)
        .build();

    // Prepare writer
    let tmpfile = NamedTempFile::new()?;
    let writer = CsvItemWriterBuilder::new()
        .has_headers(true)
        .from_writer(tmpfile.as_file());

    // Execute process with chunk size smaller than page size
    let step = StepBuilder::new("test_furniture_products")
        .chunk::<Model, Model>(1)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run();

    if let Err(ref e) = result {
        eprintln!("Job failed with error: {:?}", e);
    }
    assert!(result.is_ok());

    let step_execution = job.get_step_execution("test_furniture_products").unwrap();

    assert_eq!(step_execution.status, StepStatus::Success);
    // Should read 3 furniture products
    assert_eq!(step_execution.read_count, 3);
    assert_eq!(step_execution.write_count, 3);
    assert_eq!(step_execution.process_count, 3);

    // Verify the content is ordered by price descending
    let mut tmpfile = tmpfile.reopen()?;
    let mut file_content = String::new();
    tmpfile.read_to_string(&mut file_content)?;

    let lines: Vec<&str> = file_content.lines().collect();
    assert_eq!(lines.len(), 4); // header + 3 furniture products

    // Check that products are ordered by price (descending)
    assert!(lines[1].contains("Standing Desk")); // $399.99
    assert!(lines[2].contains("Office Chair")); // $199.99
    assert!(lines[3].contains("Desk Lamp")); // $45.99

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_orm_reader_direct() -> Result<(), Error> {
    let db = setup_test_database().await?;

    // Create a simple query
    let query = Entity::find().order_by_asc(Column::Id);

    // Create the reader without pagination
    let reader = OrmItemReaderBuilder::new()
        .connection(&db)
        .query(query)
        .build();

    // Read items directly
    let mut products = Vec::new();
    loop {
        match reader.read() {
            Ok(Some(product)) => {
                eprintln!("Read product: {} - {}", product.id, product.name);
                products.push(product);
            }
            Ok(None) => {
                eprintln!("No more products to read");
                break;
            }
            Err(e) => {
                eprintln!("Error reading product: {:?}", e);
                return Err(e.into());
            }
        }
    }

    eprintln!("Total products read: {}", products.len());
    assert_eq!(products.len(), 12);

    Ok(())
}

/// PostgreSQL test using testcontainers with improved configuration
///
/// This test uses Docker testcontainers to spin up a real PostgreSQL instance
/// for testing. It demonstrates the ORM reader working with PostgreSQL
/// using the same functionality as the SQLite tests.
///
/// Requirements:
/// - Docker must be running on the system
/// - The test creates and manages its own PostgreSQL container
///
/// To run only PostgreSQL tests:
/// ```bash
/// cargo test test_orm_reader_postgres_direct --test orm_integration
/// ```
#[tokio::test(flavor = "multi_thread")]
async fn test_orm_reader_postgres_direct() -> Result<(), Error> {
    // Create a PostgreSQL container with optimized settings
    let container = postgres::Postgres::default().start().await?;

    let host_ip = container.get_host().await?;
    let host_port = container.get_host_port_ipv4(5432).await?;

    // Wait a bit for the container to fully start
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Connect to the database with optimized connection options
    let database_url = format!(
        "postgres://postgres:postgres@{}:{}/postgres",
        host_ip, host_port
    );

    // Use more conservative connection settings for testcontainers
    let mut connect_options = sea_orm::ConnectOptions::new(&database_url);
    connect_options
        .max_connections(2) // Reduced from 5 to avoid pool contention
        .min_connections(1)
        .connect_timeout(std::time::Duration::from_secs(10))
        .acquire_timeout(std::time::Duration::from_secs(10))
        .idle_timeout(std::time::Duration::from_secs(60))
        .max_lifetime(std::time::Duration::from_secs(300));

    let db = Database::connect(connect_options).await?;

    // Set up the database with the same data as SQLite tests
    let create_table_sql = r#"
        CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            category VARCHAR(100) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            in_stock BOOLEAN NOT NULL DEFAULT true,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    "#;

    db.execute_unprepared(create_table_sql).await?;

    // Insert the same sample data as SQLite tests
    let insert_data_sql = r#"
        INSERT INTO products (name, category, price, in_stock, created_at) VALUES
        ('Laptop Pro 15', 'Electronics', 1299.99, true, '2024-01-01 10:00:00+00'),
        ('Wireless Mouse', 'Electronics', 29.99, true, '2024-01-02 11:00:00+00'),
        ('Office Chair', 'Furniture', 199.99, false, '2024-01-03 12:00:00+00'),
        ('Standing Desk', 'Furniture', 399.99, true, '2024-01-04 13:00:00+00'),
        ('Coffee Mug', 'Kitchen', 12.99, true, '2024-01-05 14:00:00+00'),
        ('Bluetooth Speaker', 'Electronics', 79.99, false, '2024-01-06 15:00:00+00'),
        ('Notebook Set', 'Office', 15.99, true, '2024-01-07 16:00:00+00'),
        ('Desk Lamp', 'Furniture', 45.99, true, '2024-01-08 17:00:00+00'),
        ('Keyboard Mechanical', 'Electronics', 129.99, true, '2024-01-09 18:00:00+00'),
        ('Water Bottle', 'Kitchen', 19.99, false, '2024-01-10 19:00:00+00'),
        ('Monitor 27inch', 'Electronics', 299.99, true, '2024-01-11 20:00:00+00'),
        ('Pen Set', 'Office', 8.99, true, '2024-01-12 21:00:00+00')
    "#;

    db.execute_unprepared(insert_data_sql).await?;

    // Create a simple query
    let query = Entity::find().order_by_asc(Column::Id);

    // Create the reader without pagination
    let reader = OrmItemReaderBuilder::new()
        .connection(&db)
        .query(query)
        .build();

    // Read items directly
    let mut products = Vec::new();
    loop {
        match reader.read() {
            Ok(Some(product)) => {
                eprintln!("Read product: {} - {}", product.id, product.name);
                products.push(product);
            }
            Ok(None) => {
                eprintln!("No more products to read");
                break;
            }
            Err(e) => {
                eprintln!("Error reading product: {:?}", e);
                return Err(e.into());
            }
        }
    }

    eprintln!("Total products read: {}", products.len());
    assert_eq!(products.len(), 12);

    // Explicitly close the database connection before dropping the container
    db.close().await?;

    // Keep the container alive until the end of the test
    drop(container);

    Ok(())
}

/// PostgreSQL test with pagination using testcontainers
///
/// This test verifies that pagination works correctly with PostgreSQL,
/// specifically testing filtered queries with small page sizes to ensure
/// the pagination logic works properly across multiple database pages.
///
/// The test filters for in-stock products only and uses a small page size
/// to force multiple database queries, verifying the pagination state
/// is maintained correctly.
#[tokio::test(flavor = "multi_thread")]
async fn test_orm_reader_postgres_with_pagination() -> Result<(), Error> {
    // Create a PostgreSQL container
    let container = postgres::Postgres::default().start().await?;

    let host_ip = container.get_host().await?;
    let host_port = container.get_host_port_ipv4(5432).await?;

    // Wait for container to be ready
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    // Connect to the database
    let database_url = format!(
        "postgres://postgres:postgres@{}:{}/postgres",
        host_ip, host_port
    );

    let mut connect_options = sea_orm::ConnectOptions::new(&database_url);
    connect_options
        .max_connections(2)
        .min_connections(1)
        .connect_timeout(std::time::Duration::from_secs(10))
        .acquire_timeout(std::time::Duration::from_secs(10))
        .idle_timeout(std::time::Duration::from_secs(60))
        .max_lifetime(std::time::Duration::from_secs(300));

    let db = Database::connect(connect_options).await?;

    // Create table and insert test data
    let create_table_sql = r#"
        CREATE TABLE products (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            category VARCHAR(100) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            in_stock BOOLEAN NOT NULL DEFAULT true,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    "#;

    db.execute_unprepared(create_table_sql).await?;

    let insert_data_sql = r#"
        INSERT INTO products (name, category, price, in_stock, created_at) VALUES
        ('Laptop Pro 15', 'Electronics', 1299.99, true, '2024-01-01 10:00:00+00'),
        ('Wireless Mouse', 'Electronics', 29.99, true, '2024-01-02 11:00:00+00'),
        ('Office Chair', 'Furniture', 199.99, false, '2024-01-03 12:00:00+00'),
        ('Standing Desk', 'Furniture', 399.99, true, '2024-01-04 13:00:00+00'),
        ('Coffee Mug', 'Kitchen', 12.99, true, '2024-01-05 14:00:00+00')
    "#;

    db.execute_unprepared(insert_data_sql).await?;

    // Test with pagination - filter for in-stock items only
    let query = Entity::find()
        .filter(Column::InStock.eq(true))
        .order_by_asc(Column::Id);

    let reader = OrmItemReaderBuilder::new()
        .connection(&db)
        .query(query)
        .page_size(2) // Small page size to test pagination
        .build();

    // Read all in-stock products
    let mut products = Vec::new();
    loop {
        match reader.read() {
            Ok(Some(product)) => {
                eprintln!(
                    "Read product: {} - {} (in stock: {})",
                    product.id, product.name, product.in_stock
                );
                assert!(product.in_stock); // Ensure we only get in-stock items
                products.push(product);
            }
            Ok(None) => break,
            Err(e) => {
                eprintln!("Error reading product: {:?}", e);
                return Err(e.into());
            }
        }
    }

    // Should have 4 in-stock products (all except Office Chair)
    eprintln!("Total in-stock products read: {}", products.len());
    assert_eq!(products.len(), 4);

    // Verify the products are in correct order
    assert_eq!(products[0].name, "Laptop Pro 15");
    assert_eq!(products[1].name, "Wireless Mouse");
    assert_eq!(products[2].name, "Standing Desk");
    assert_eq!(products[3].name, "Coffee Mug");

    // Close database connection
    db.close().await?;
    drop(container);

    Ok(())
}

/// Tests for ORM Item Writer functionality
mod orm_writer_tests {
    use super::*;
    use sea_orm::{ActiveValue::Set, EntityTrait, QueryFilter};

    /// Processor that converts ProductInsertDto to ORM ActiveModel
    #[derive(Default)]
    struct ProductDtoToActiveModelProcessor;

    impl ItemProcessor<ProductInsertDto, ActiveModel> for ProductDtoToActiveModelProcessor {
        fn process(&self, item: ProductInsertDto) -> ItemProcessorResult<ActiveModel> {
            let active_model = ActiveModel {
                id: sea_orm::ActiveValue::NotSet, // Auto-generated
                name: Set(item.name),
                category: Set(item.category),
                price: Set(item.price),
                in_stock: Set(item.in_stock),
                created_at: Set(DateTimeUtc::default()),
            };
            Ok(Some(active_model))
        }
    }

    /// Product DTO for insertion
    #[derive(Debug, Clone, Serialize)]
    pub struct ProductInsertDto {
        pub name: String,
        pub category: String,
        pub price: Decimal,
        pub in_stock: bool,
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_orm_writer_basic_functionality() -> Result<(), Error> {
        let db = setup_test_database().await?;

        // Clear existing data
        Entity::delete_many().exec(&db).await?;

        // Create writer - no mapper needed!
        let writer = OrmItemWriterBuilder::<ActiveModel>::new()
            .connection(&db)
            .build();

        // Create test active models directly
        let active_models = vec![
            ActiveModel {
                id: sea_orm::ActiveValue::NotSet,
                name: Set("Gaming Mouse".to_string()),
                category: Set("Electronics".to_string()),
                price: Set(Decimal::from_str("49.99").unwrap()),
                in_stock: Set(true),
                created_at: Set(DateTimeUtc::default()),
            },
            ActiveModel {
                id: sea_orm::ActiveValue::NotSet,
                name: Set("Coffee Table".to_string()),
                category: Set("Furniture".to_string()),
                price: Set(Decimal::from_str("199.99").unwrap()),
                in_stock: Set(false),
                created_at: Set(DateTimeUtc::default()),
            },
        ];

        // Write active models to database
        writer.write(&active_models)?;

        // Verify products were inserted
        let saved_products: Vec<Model> = Entity::find().all(&db).await?;

        assert_eq!(saved_products.len(), 2);

        // Find and verify the gaming mouse
        let gaming_mouse = saved_products
            .iter()
            .find(|p| p.name == "Gaming Mouse")
            .expect("Gaming Mouse not found");
        assert_eq!(gaming_mouse.category, "Electronics");
        assert_eq!(gaming_mouse.price, Decimal::from_str("49.99").unwrap());
        assert!(gaming_mouse.in_stock);

        // Find and verify the coffee table
        let coffee_table = saved_products
            .iter()
            .find(|p| p.name == "Coffee Table")
            .expect("Coffee Table not found");
        assert_eq!(coffee_table.category, "Furniture");
        assert_eq!(coffee_table.price, Decimal::from_str("199.99").unwrap());
        assert!(!coffee_table.in_stock);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_orm_writer_empty_batch() -> Result<(), Error> {
        let db = setup_test_database().await?;

        // Clear existing data
        Entity::delete_many().exec(&db).await?;

        // Create writer
        let writer = OrmItemWriterBuilder::<ActiveModel>::new()
            .connection(&db)
            .build();

        // Write empty batch
        let active_models: Vec<ActiveModel> = vec![];
        writer.write(&active_models)?;

        // Verify no products were inserted
        let saved_products: Vec<Model> = Entity::find().all(&db).await?;
        assert_eq!(saved_products.len(), 0);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_orm_writer_large_batch() -> Result<(), Error> {
        let db = setup_test_database().await?;

        // Clear existing data
        Entity::delete_many().exec(&db).await?;

        // Create writer
        let writer = OrmItemWriterBuilder::<ActiveModel>::new()
            .connection(&db)
            .build();

        // Create a large batch of active models
        let mut active_models = Vec::new();
        for i in 0..50 {
            active_models.push(ActiveModel {
                id: sea_orm::ActiveValue::NotSet,
                name: Set(format!("Product {}", i)),
                category: Set(if i % 2 == 0 { "Even" } else { "Odd" }.to_string()),
                price: Set(Decimal::from_str(&format!("{}.99", i)).unwrap()),
                in_stock: Set(i % 3 == 0),
                created_at: Set(DateTimeUtc::default()),
            });
        }

        // Write active models to database
        writer.write(&active_models)?;

        // Verify all products were inserted
        let saved_products: Vec<Model> = Entity::find().all(&db).await?;
        assert_eq!(saved_products.len(), 50);

        // Verify some specific products
        let product_0 = saved_products
            .iter()
            .find(|p| p.name == "Product 0")
            .expect("Product 0 not found");
        assert_eq!(product_0.category, "Even");
        assert!(product_0.in_stock);

        let product_25 = saved_products
            .iter()
            .find(|p| p.name == "Product 25")
            .expect("Product 25 not found");
        assert_eq!(product_25.category, "Odd");
        assert!(!product_25.in_stock);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_orm_writer_with_special_characters() -> Result<(), Error> {
        let db = setup_test_database().await?;

        // Clear existing data
        Entity::delete_many().exec(&db).await?;

        // Create writer
        let writer = OrmItemWriterBuilder::<ActiveModel>::new()
            .connection(&db)
            .build();

        // Create active models with special characters
        let active_models = vec![
            ActiveModel {
                id: sea_orm::ActiveValue::NotSet,
                name: Set("Café Français™".to_string()),
                category: Set("Food & Drink".to_string()),
                price: Set(Decimal::from_str("12.50").unwrap()),
                in_stock: Set(true),
                created_at: Set(DateTimeUtc::default()),
            },
            ActiveModel {
                id: sea_orm::ActiveValue::NotSet,
                name: Set("🚀 Rocket Emoji Product 📱".to_string()),
                category: Set("Technology™".to_string()),
                price: Set(Decimal::from_str("999.99").unwrap()),
                in_stock: Set(false),
                created_at: Set(DateTimeUtc::default()),
            },
        ];

        // Write active models to database
        writer.write(&active_models)?;

        // Verify products were inserted correctly
        let saved_products: Vec<Model> = Entity::find().all(&db).await?;
        assert_eq!(saved_products.len(), 2);

        let cafe_product = saved_products
            .iter()
            .find(|p| p.name.contains("Café"))
            .expect("Café product not found");
        assert_eq!(cafe_product.name, "Café Français™");
        assert_eq!(cafe_product.category, "Food & Drink");

        let emoji_product = saved_products
            .iter()
            .find(|p| p.name.contains("🚀"))
            .expect("Emoji product not found");
        assert_eq!(emoji_product.name, "🚀 Rocket Emoji Product 📱");
        assert_eq!(emoji_product.category, "Technology™");

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_orm_writer_integration_with_job() -> Result<(), Error> {
        let db = setup_test_database().await?;

        // Clear existing data
        Entity::delete_many().exec(&db).await?;

        // Create a simple reader that provides DTOs
        struct SimpleProductReader {
            products: Vec<ProductInsertDto>,
            position: Cell<usize>,
        }

        impl ItemReader<ProductInsertDto> for SimpleProductReader {
            fn read(&self) -> Result<Option<ProductInsertDto>, BatchError> {
                let pos = self.position.get();
                if pos < self.products.len() {
                    let product = self.products[pos].clone();
                    self.position.set(pos + 1);
                    Ok(Some(product))
                } else {
                    Ok(None)
                }
            }
        }

        // Create test data
        let test_products = vec![
            ProductInsertDto {
                name: "Integration Product 1".to_string(),
                category: "Test".to_string(),
                price: Decimal::from_str("10.00").unwrap(),
                in_stock: true,
            },
            ProductInsertDto {
                name: "Integration Product 2".to_string(),
                category: "Test".to_string(),
                price: Decimal::from_str("20.00").unwrap(),
                in_stock: false,
            },
            ProductInsertDto {
                name: "Integration Product 3".to_string(),
                category: "Test".to_string(),
                price: Decimal::from_str("30.00").unwrap(),
                in_stock: true,
            },
        ];

        let reader = SimpleProductReader {
            products: test_products,
            position: Cell::new(0),
        };

        // Create writer
        let writer = OrmItemWriterBuilder::<ActiveModel>::new()
            .connection(&db)
            .build();

        // Use processor to convert DTOs to active models
        let processor = ProductDtoToActiveModelProcessor;

        // Create and run job
        let step = StepBuilder::new("test_orm_writer_integration")
            .chunk::<ProductInsertDto, ActiveModel>(2)
            .reader(&reader)
            .processor(&processor)
            .writer(&writer)
            .build();

        let job = JobBuilder::new().start(&step).build();
        let result = job.run();

        if let Err(ref e) = result {
            eprintln!("Job failed with error: {:?}", e);
        }
        assert!(result.is_ok());

        // Verify job execution results
        let step_execution = job
            .get_step_execution("test_orm_writer_integration")
            .unwrap();
        assert_eq!(step_execution.status, StepStatus::Success);
        assert_eq!(step_execution.read_count, 3);
        assert_eq!(step_execution.write_count, 3);
        assert_eq!(step_execution.process_count, 3);
        assert_eq!(step_execution.read_error_count, 0);
        assert_eq!(step_execution.write_error_count, 0);

        // Verify products were written to database
        let saved_products: Vec<Model> = Entity::find()
            .filter(Column::Category.eq("Test"))
            .all(&db)
            .await?;

        assert_eq!(saved_products.len(), 3);

        // Verify specific products
        let product_names: Vec<&String> = saved_products.iter().map(|p| &p.name).collect();
        assert!(product_names.contains(&&"Integration Product 1".to_string()));
        assert!(product_names.contains(&&"Integration Product 2".to_string()));
        assert!(product_names.contains(&&"Integration Product 3".to_string()));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_orm_writer_lifecycle_methods() -> Result<(), Error> {
        let db = setup_test_database().await?;

        // Create writer
        let writer = OrmItemWriterBuilder::<ActiveModel>::new()
            .connection(&db)
            .build();

        // Test lifecycle methods
        assert!(writer.open().is_ok());
        assert!(writer.flush().is_ok());
        assert!(writer.close().is_ok());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_orm_writer_with_extreme_values() -> Result<(), Error> {
        let db = setup_test_database().await?;

        // Clear existing data
        Entity::delete_many().exec(&db).await?;

        // Create writer
        let writer = OrmItemWriterBuilder::<ActiveModel>::new()
            .connection(&db)
            .build();

        // Create active models with extreme values
        let active_models = vec![
            ActiveModel {
                id: sea_orm::ActiveValue::NotSet,
                name: Set("".to_string()),      // Empty name
                category: Set("x".repeat(100)), // Very long category
                price: Set(Decimal::from_str("0.01").unwrap()), // Minimum price
                in_stock: Set(true),
                created_at: Set(DateTimeUtc::default()),
            },
            ActiveModel {
                id: sea_orm::ActiveValue::NotSet,
                name: Set("z".repeat(255)),    // Very long name
                category: Set("".to_string()), // Empty category
                price: Set(Decimal::from_str("999999.99").unwrap()), // Large price
                in_stock: Set(false),
                created_at: Set(DateTimeUtc::default()),
            },
        ];

        // Write active models to database
        writer.write(&active_models)?;

        // Verify products were inserted
        let saved_products: Vec<Model> = Entity::find().all(&db).await?;
        assert_eq!(saved_products.len(), 2);

        // Find products by unique characteristics
        let empty_name_product = saved_products
            .iter()
            .find(|p| p.name.is_empty())
            .expect("Empty name product not found");
        assert_eq!(empty_name_product.category, "x".repeat(100));
        assert_eq!(empty_name_product.price, Decimal::from_str("0.01").unwrap());

        let long_name_product = saved_products
            .iter()
            .find(|p| p.name.len() == 255)
            .expect("Long name product not found");
        assert_eq!(long_name_product.category, "");
        assert_eq!(
            long_name_product.price,
            Decimal::from_str("999999.99").unwrap()
        );

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_orm_writer_read_write_round_trip() -> Result<(), Error> {
        let db = setup_test_database().await?;

        // Clear existing data
        Entity::delete_many().exec(&db).await?;

        // Create writer
        let writer = OrmItemWriterBuilder::<ActiveModel>::new()
            .connection(&db)
            .build();

        // Create test active models for writing
        let active_models_to_write = vec![
            ActiveModel {
                id: sea_orm::ActiveValue::NotSet,
                name: Set("Round Trip Product 1".to_string()),
                category: Set("Round Trip".to_string()),
                price: Set(Decimal::from_str("123.45").unwrap()),
                in_stock: Set(true),
                created_at: Set(DateTimeUtc::default()),
            },
            ActiveModel {
                id: sea_orm::ActiveValue::NotSet,
                name: Set("Round Trip Product 2".to_string()),
                category: Set("Round Trip".to_string()),
                price: Set(Decimal::from_str("678.90").unwrap()),
                in_stock: Set(false),
                created_at: Set(DateTimeUtc::default()),
            },
        ];

        // Write active models to database
        writer.write(&active_models_to_write)?;

        // Now read them back using the existing reader
        let query = Entity::find()
            .filter(Column::Category.eq("Round Trip"))
            .order_by_asc(Column::Name);

        let reader = OrmItemReaderBuilder::new()
            .connection(&db)
            .query(query)
            .build();

        // Read all products back
        let mut read_products = Vec::new();
        loop {
            match reader.read() {
                Ok(Some(product)) => read_products.push(product),
                Ok(None) => break,
                Err(e) => return Err(e.into()),
            }
        }

        // Verify we read back the same number of products
        assert_eq!(read_products.len(), 2);

        // Verify the data matches (accounting for auto-generated IDs and timestamps)
        let product1 = &read_products[0];
        assert_eq!(product1.name, "Round Trip Product 1");
        assert_eq!(product1.category, "Round Trip");
        assert_eq!(product1.price, Decimal::from_str("123.45").unwrap());
        assert!(product1.in_stock);

        let product2 = &read_products[1];
        assert_eq!(product2.name, "Round Trip Product 2");
        assert_eq!(product2.category, "Round Trip");
        assert_eq!(product2.price, Decimal::from_str("678.90").unwrap());
        assert!(!product2.in_stock);

        Ok(())
    }

    /// PostgreSQL integration test for ORM item writer using testcontainers
    ///
    /// This test verifies that the ORM writer works correctly with PostgreSQL,
    /// demonstrating real-world database integration with proper container management.
    /// The container reference is kept alive throughout the test to ensure the
    /// PostgreSQL instance remains available.
    ///
    /// Requirements:
    /// - Docker must be running on the system
    /// - The test creates and manages its own PostgreSQL container
    ///
    /// To run only this PostgreSQL writer test:
    /// ```bash
    /// cargo test test_orm_writer_postgres_integration --test orm_integration
    /// ```
    #[tokio::test(flavor = "multi_thread")]
    async fn test_orm_writer_postgres_integration() -> Result<(), Error> {
        // Create a PostgreSQL container with optimized settings
        let container = postgres::Postgres::default().start().await?;

        let host_ip = container.get_host().await?;
        let host_port = container.get_host_port_ipv4(5432).await?;

        // Wait for the container to fully start
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Connect to the database with optimized connection options
        let database_url = format!(
            "postgres://postgres:postgres@{}:{}/postgres",
            host_ip, host_port
        );

        let mut connect_options = sea_orm::ConnectOptions::new(&database_url);
        connect_options
            .max_connections(2)
            .min_connections(1)
            .connect_timeout(std::time::Duration::from_secs(10))
            .acquire_timeout(std::time::Duration::from_secs(10))
            .idle_timeout(std::time::Duration::from_secs(60))
            .max_lifetime(std::time::Duration::from_secs(300));

        let db = Database::connect(connect_options).await?;

        // Create the products table with PostgreSQL-specific syntax
        let create_table_sql = r#"
            CREATE TABLE products (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                description VARCHAR(255),
                category VARCHAR(100) NOT NULL,
                price DECIMAL(10,2) NOT NULL,
                in_stock BOOLEAN NOT NULL DEFAULT true,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        "#;

        db.execute_unprepared(create_table_sql).await?;

        // Create writer
        let writer = OrmItemWriterBuilder::<ActiveModel>::new()
            .connection(&db)
            .build();

        // Create test active models with PostgreSQL-specific considerations
        let active_models = vec![
            ActiveModel {
                id: sea_orm::ActiveValue::NotSet, // PostgreSQL will auto-generate with SERIAL
                name: Set("PostgreSQL Gaming Laptop".to_string()),
                category: Set("Electronics".to_string()),
                price: Set(Decimal::from_str("1599.99").unwrap()),
                in_stock: Set(true),
                created_at: Set(DateTimeUtc::default()),
            },
            ActiveModel {
                id: sea_orm::ActiveValue::NotSet,
                name: Set("PostgreSQL Office Desk".to_string()),
                category: Set("Furniture".to_string()),
                price: Set(Decimal::from_str("299.99").unwrap()),
                in_stock: Set(false),
                created_at: Set(DateTimeUtc::default()),
            },
            ActiveModel {
                id: sea_orm::ActiveValue::NotSet,
                name: Set("PostgreSQL Coffee Maker ☕".to_string()), // Test Unicode support
                category: Set("Kitchen & Dining".to_string()),
                price: Set(Decimal::from_str("89.99").unwrap()),
                in_stock: Set(true),
                created_at: Set(DateTimeUtc::default()),
            },
        ];

        // Write active models to PostgreSQL database
        eprintln!("Writing {} products to PostgreSQL...", active_models.len());
        writer.write(&active_models)?;

        // Verify products were inserted correctly
        let saved_products: Vec<Model> = Entity::find().order_by_asc(Column::Id).all(&db).await?;

        eprintln!(
            "Retrieved {} products from PostgreSQL",
            saved_products.len()
        );
        assert_eq!(saved_products.len(), 3);

        // Verify the gaming laptop
        let gaming_laptop = saved_products
            .iter()
            .find(|p| p.name.contains("Gaming Laptop"))
            .expect("Gaming Laptop not found");
        assert_eq!(gaming_laptop.category, "Electronics");
        assert_eq!(gaming_laptop.price, Decimal::from_str("1599.99").unwrap());
        assert!(gaming_laptop.in_stock);
        assert!(gaming_laptop.id > 0); // PostgreSQL auto-generated ID

        // Verify the office desk
        let office_desk = saved_products
            .iter()
            .find(|p| p.name.contains("Office Desk"))
            .expect("Office Desk not found");
        assert_eq!(office_desk.category, "Furniture");
        assert_eq!(office_desk.price, Decimal::from_str("299.99").unwrap());
        assert!(!office_desk.in_stock);

        // Verify the coffee maker with Unicode
        let coffee_maker = saved_products
            .iter()
            .find(|p| p.name.contains("Coffee Maker"))
            .expect("Coffee Maker not found");
        assert_eq!(coffee_maker.category, "Kitchen & Dining");
        assert_eq!(coffee_maker.price, Decimal::from_str("89.99").unwrap());
        assert!(coffee_maker.in_stock);
        assert!(coffee_maker.name.contains("☕")); // Verify Unicode was preserved

        // Test batch writing with larger dataset
        eprintln!("Testing batch writing with larger dataset...");
        let mut large_batch = Vec::new();
        for i in 0..25 {
            large_batch.push(ActiveModel {
                id: sea_orm::ActiveValue::NotSet,
                name: Set(format!("Batch Product {}", i)),
                category: Set(if i % 3 == 0 {
                    "Category A"
                } else if i % 3 == 1 {
                    "Category B"
                } else {
                    "Category C"
                }
                .to_string()),
                price: Set(Decimal::from_str(&format!("{}.99", 10 + i)).unwrap()),
                in_stock: Set(i % 2 == 0),
                created_at: Set(DateTimeUtc::default()),
            });
        }

        writer.write(&large_batch)?;

        // Verify the batch was written correctly
        let total_products: Vec<Model> = Entity::find().all(&db).await?;
        assert_eq!(total_products.len(), 28); // 3 initial + 25 batch

        // Verify some batch products
        let batch_products: Vec<Model> = Entity::find()
            .filter(Column::Name.like("Batch Product%"))
            .order_by_asc(Column::Name)
            .all(&db)
            .await?;
        assert_eq!(batch_products.len(), 25);

        // Test specific batch product
        let batch_product_10 = batch_products
            .iter()
            .find(|p| p.name == "Batch Product 10")
            .expect("Batch Product 10 not found");
        assert_eq!(batch_product_10.price, Decimal::from_str("20.99").unwrap());
        assert!(batch_product_10.in_stock); // 10 % 2 == 0

        // Test writer lifecycle methods
        eprintln!("Testing writer lifecycle methods...");
        assert!(writer.open().is_ok());
        assert!(writer.flush().is_ok());
        assert!(writer.close().is_ok());

        // Test empty batch writing
        eprintln!("Testing empty batch writing...");
        let empty_batch: Vec<ActiveModel> = vec![];
        writer.write(&empty_batch)?; // Should not fail

        // Final verification - count should remain the same
        let final_count = Entity::find().count(&db).await?;
        assert_eq!(final_count, 28);

        eprintln!("PostgreSQL ORM writer integration test completed successfully!");

        // Explicitly close the database connection before dropping the container
        db.close().await?;

        // Keep the container reference alive until the end of the test
        // This ensures the PostgreSQL instance remains available throughout the test
        drop(container);

        Ok(())
    }
}
