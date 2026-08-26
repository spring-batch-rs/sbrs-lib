//! # ORM Processing Examples (SeaORM)
//!
//! Demonstrates reading from and writing to databases using SeaORM with Spring Batch RS.
//! Uses SQLite in-memory database (no external database required).
//!
//! ## Features Demonstrated
//! - Reading entities with SeaORM queries
//! - Pagination for large datasets
//! - Filtering and ordering with SeaORM
//! - Writing entities directly to database
//! - Converting between business DTOs and ORM entities
//!
//! ## Run
//! ```bash
//! cargo run --example orm_processing --features orm,csv,json
//! ```

use sea_orm::{
    ActiveValue::Set, Database, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder,
    entity::prelude::*,
};
use serde::{Deserialize, Serialize};
use spring_batch_rs::{
    BatchError,
    core::{
        item::{ItemProcessor, ItemReader},
        job::{Job, JobBuilder},
        step::StepBuilder,
    },
    item::{
        csv::csv_writer::CsvItemWriterBuilder,
        json::json_writer::JsonItemWriterBuilder,
        orm::{OrmItemReaderBuilder, OrmItemWriterBuilder},
    },
};
use std::env::temp_dir;

// =============================================================================
// ORM Entity Definition
// =============================================================================

/// SeaORM entity for the `products` table.
mod products {
    use sea_orm::entity::prelude::*;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel, Deserialize, Serialize)]
    #[sea_orm(table_name = "products")]
    pub struct Model {
        #[sea_orm(primary_key)]
        pub id: i32,
        pub name: String,
        pub category: String,
        pub price: f64,
        pub in_stock: bool,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

// =============================================================================
// Data Structures
// =============================================================================

/// A business DTO for product data.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct ProductDto {
    id: i32,
    name: String,
    category: String,
    price: f64,
    in_stock: bool,
}

/// A CSV-friendly product record.
#[derive(Debug, Serialize)]
struct ProductCsv {
    id: i32,
    name: String,
    category: String,
    price: f64,
}

/// Processor that converts Product Model to CSV format.
struct ProductToCsvProcessor;

impl ItemProcessor<products::Model, ProductCsv> for ProductToCsvProcessor {
    fn process(&self, item: products::Model) -> Result<Option<ProductCsv>, BatchError> {
        Ok(Some(ProductCsv {
            id: item.id,
            name: item.name,
            category: item.category,
            price: item.price,
        }))
    }
}

/// Processor that converts ProductDto to SeaORM ActiveModel.
struct DtoToActiveModelProcessor;

impl ItemProcessor<ProductDto, products::ActiveModel> for DtoToActiveModelProcessor {
    fn process(&self, item: ProductDto) -> Result<Option<products::ActiveModel>, BatchError> {
        Ok(Some(products::ActiveModel {
            id: Set(item.id),
            name: Set(item.name),
            category: Set(item.category),
            price: Set(item.price),
            in_stock: Set(item.in_stock),
        }))
    }
}

// =============================================================================
// Database Setup
// =============================================================================

/// Creates and seeds the SQLite in-memory database.
async fn setup_database() -> Result<DatabaseConnection, DbErr> {
    let db = Database::connect("sqlite::memory:").await?;

    // Create products table
    db.execute_unprepared(
        r#"
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL,
            in_stock BOOLEAN NOT NULL DEFAULT 1
        )
        "#,
    )
    .await?;

    // Seed products
    db.execute_unprepared(
        r#"
        INSERT INTO products (id, name, category, price, in_stock) VALUES
        (1, 'Laptop Pro', 'Electronics', 1299.99, 1),
        (2, 'Wireless Mouse', 'Electronics', 49.99, 1),
        (3, 'USB-C Hub', 'Electronics', 79.99, 0),
        (4, 'Desk Chair', 'Furniture', 299.99, 1),
        (5, 'Standing Desk', 'Furniture', 599.99, 1),
        (6, 'Monitor Arm', 'Furniture', 129.99, 0),
        (7, 'Notebook Set', 'Office', 24.99, 1),
        (8, 'Pen Collection', 'Office', 39.99, 1),
        (9, 'Desk Organizer', 'Office', 44.99, 1),
        (10, 'Webcam HD', 'Electronics', 89.99, 1)
        "#,
    )
    .await?;

    Ok(db)
}

// =============================================================================
// Example 1: Read All Products
// =============================================================================

/// Reads all products and exports to JSON.
fn example_read_all_to_json(db: &DatabaseConnection) -> Result<(), BatchError> {
    println!("=== Example 1: Read All Products to JSON ===");

    let query = products::Entity::find().order_by_asc(products::Column::Id);

    let reader = OrmItemReaderBuilder::new()
        .connection(db)
        .query(query)
        .page_size(5)
        .build();

    let output_path = temp_dir().join("all_products.json");
    let writer = JsonItemWriterBuilder::<products::Model>::new()
        .pretty_formatter(true)
        .from_path(&output_path);

    let step = StepBuilder::new("read-all-products")
        .chunk::<products::Model, products::Model>(5)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run()?;

    let step_exec = job.get_step_execution("read-all-products").unwrap();
    println!("  Products read: {}", step_exec.read_count);
    println!("  Output: {}", output_path.display());
    println!("  Duration: {:?}", result.duration);
    Ok(())
}

// =============================================================================
// Example 2: Read with Filter
// =============================================================================

/// Reads only in-stock electronics and exports to CSV.
fn example_read_filtered_to_csv(db: &DatabaseConnection) -> Result<(), BatchError> {
    println!("\n=== Example 2: Read Filtered to CSV ===");

    let query = products::Entity::find()
        .filter(products::Column::Category.eq("Electronics"))
        .filter(products::Column::InStock.eq(true))
        .order_by_asc(products::Column::Name);

    let reader = OrmItemReaderBuilder::new()
        .connection(db)
        .query(query)
        .build();

    let output_path = temp_dir().join("electronics_in_stock.csv");
    let writer = CsvItemWriterBuilder::<ProductCsv>::new()
        .has_headers(true)
        .from_path(&output_path);

    let processor = ProductToCsvProcessor;

    let step = StepBuilder::new("filter-electronics")
        .chunk::<products::Model, ProductCsv>(10)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    job.run()?;

    println!("  Exported in-stock electronics to CSV");
    println!("  Output: {}", output_path.display());
    Ok(())
}

// =============================================================================
// Example 3: Read with Complex Query
// =============================================================================

/// Reads products over a price threshold.
fn example_read_expensive_products(db: &DatabaseConnection) -> Result<(), BatchError> {
    println!("\n=== Example 3: Read Expensive Products (price >= $100) ===");

    let query = products::Entity::find()
        .filter(products::Column::Price.gte(100.0))
        .order_by_desc(products::Column::Price);

    let reader = OrmItemReaderBuilder::new()
        .connection(db)
        .query(query)
        .page_size(3)
        .build();

    let output_path = temp_dir().join("expensive_products.json");
    let writer = JsonItemWriterBuilder::<products::Model>::new()
        .pretty_formatter(true)
        .from_path(&output_path);

    let step = StepBuilder::new("expensive-products")
        .chunk::<products::Model, products::Model>(10)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run()?;

    let step_exec = job.get_step_execution("expensive-products").unwrap();
    println!("  Expensive products found: {}", step_exec.read_count);
    println!("  Output: {}", output_path.display());
    println!("  Duration: {:?}", result.duration);
    Ok(())
}

// =============================================================================
// Example 4: Write to Database
// =============================================================================

/// Writes new products to the database from DTOs.
fn example_write_to_database(db: &DatabaseConnection) -> Result<(), BatchError> {
    println!("\n=== Example 4: Write Products to Database ===");

    // Create a simple in-memory reader for new products
    let new_products = vec![
        ProductDto {
            id: 11,
            name: "Mechanical Keyboard".to_string(),
            category: "Electronics".to_string(),
            price: 149.99,
            in_stock: true,
        },
        ProductDto {
            id: 12,
            name: "Monitor Stand".to_string(),
            category: "Furniture".to_string(),
            price: 79.99,
            in_stock: true,
        },
        ProductDto {
            id: 13,
            name: "Cable Management Kit".to_string(),
            category: "Office".to_string(),
            price: 29.99,
            in_stock: true,
        },
    ];

    // Use a simple wrapper reader
    let reader = InMemoryReader::new(new_products);

    let writer = OrmItemWriterBuilder::<products::ActiveModel>::new()
        .connection(db)
        .build();

    let processor = DtoToActiveModelProcessor;

    let step = StepBuilder::new("write-products")
        .chunk::<ProductDto, products::ActiveModel>(2)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run()?;

    let step_exec = job.get_step_execution("write-products").unwrap();
    println!("  Products written: {}", step_exec.write_count);
    println!("  Duration: {:?}", result.duration);
    Ok(())
}

// =============================================================================
// Example 5: Verify Written Data
// =============================================================================

/// Verifies the data written in Example 4.
fn example_verify_written_data(db: &DatabaseConnection) -> Result<(), BatchError> {
    println!("\n=== Example 5: Verify Written Data ===");

    let query = products::Entity::find()
        .filter(products::Column::Id.gte(11))
        .order_by_asc(products::Column::Id);

    let reader = OrmItemReaderBuilder::new()
        .connection(db)
        .query(query)
        .build();

    let output_path = temp_dir().join("new_products.json");
    let writer = JsonItemWriterBuilder::<products::Model>::new()
        .pretty_formatter(true)
        .from_path(&output_path);

    let step = StepBuilder::new("verify-written")
        .chunk::<products::Model, products::Model>(10)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run()?;

    let step_exec = job.get_step_execution("verify-written").unwrap();
    println!("  New products found: {}", step_exec.read_count);
    println!("  Output: {}", output_path.display());
    println!("  Duration: {:?}", result.duration);
    Ok(())
}

// =============================================================================
// Helper: In-Memory Reader
// =============================================================================

/// A simple in-memory reader for demonstration purposes.
struct InMemoryReader<T> {
    items: std::cell::RefCell<std::collections::VecDeque<T>>,
}

impl<T: Clone> InMemoryReader<T> {
    fn new(items: Vec<T>) -> Self {
        Self {
            items: std::cell::RefCell::new(items.into()),
        }
    }
}

impl<T: Clone> ItemReader<T> for InMemoryReader<T> {
    fn read(&self) -> Result<Option<T>, BatchError> {
        Ok(self.items.borrow_mut().pop_front())
    }
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<(), BatchError> {
    env_logger::init();

    println!("ORM Processing Examples (SeaORM + SQLite)");
    println!("=========================================\n");

    // Setup database
    let db = setup_database()
        .await
        .map_err(|e| BatchError::ItemReader(format!("Failed to setup database: {}", e)))?;

    println!("Database initialized with sample products.\n");

    // Run examples
    example_read_all_to_json(&db)?;
    example_read_filtered_to_csv(&db)?;
    example_read_expensive_products(&db)?;
    example_write_to_database(&db)?;
    example_verify_written_data(&db)?;

    println!("\n✓ All ORM examples completed successfully!");
    Ok(())
}
