//! # CSV Processing Examples
//!
//! Demonstrates reading and writing CSV files with Spring Batch RS.
//!
//! ## Features Demonstrated
//! - Reading CSV from files and in-memory strings
//! - Writing CSV with and without headers
//! - Custom delimiters
//! - Data transformation with processors
//! - Fault tolerance with skip limits
//!
//! ## Run
//! ```bash
//! cargo run --example csv_processing --features csv,json
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
        csv::csv_reader::CsvItemReaderBuilder, csv::csv_writer::CsvItemWriterBuilder,
        json::json_writer::JsonItemWriterBuilder,
    },
};
use std::env::temp_dir;

// =============================================================================
// Data Structures
// =============================================================================

/// A product record for CSV processing.
#[derive(Debug, Deserialize, Serialize)]
struct Product {
    id: u32,
    name: String,
    price: f64,
    category: String,
}

/// Processor that applies a discount to product prices.
struct DiscountProcessor {
    discount_percent: f64,
}

impl DiscountProcessor {
    fn new(discount_percent: f64) -> Self {
        Self { discount_percent }
    }
}

impl ItemProcessor<Product, Product> for DiscountProcessor {
    fn process(&self, item: Product) -> Result<Option<Product>, BatchError> {
        Ok(Some(Product {
            id: item.id,
            name: item.name,
            price: item.price * (1.0 - self.discount_percent / 100.0),
            category: item.category,
        }))
    }
}

// =============================================================================
// Example 1: Basic CSV to CSV
// =============================================================================

/// Reads a CSV file and writes to another CSV file with headers.
fn example_csv_to_csv() -> Result<(), BatchError> {
    println!("=== Example 1: Basic CSV to CSV ===");

    let csv_data = "\
id,name,price,category
1,Laptop,999.99,Electronics
2,Coffee Mug,12.99,Kitchen
3,Notebook,5.99,Office
4,Headphones,79.99,Electronics";

    let reader = CsvItemReaderBuilder::<Product>::new()
        .has_headers(true)
        .from_reader(csv_data.as_bytes());

    let output_path = temp_dir().join("products_copy.csv");
    let writer = CsvItemWriterBuilder::<Product>::new()
        .has_headers(true)
        .from_path(&output_path);

    let step = StepBuilder::new("csv-to-csv")
        .chunk::<Product, Product>(2)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run()?;

    println!("  Output: {}", output_path.display());
    println!("  Duration: {:?}", result.duration);
    Ok(())
}

// =============================================================================
// Example 2: CSV to JSON with Transformation
// =============================================================================

/// Reads CSV, applies a discount transformation, and writes to JSON.
fn example_csv_to_json_with_processor() -> Result<(), BatchError> {
    println!("\n=== Example 2: CSV to JSON with Processor ===");

    let csv_data = "\
id,name,price,category
1,Laptop,999.99,Electronics
2,Coffee Mug,12.99,Kitchen
3,Notebook,5.99,Office";

    let reader = CsvItemReaderBuilder::<Product>::new()
        .has_headers(true)
        .from_reader(csv_data.as_bytes());

    let output_path = temp_dir().join("products_discounted.json");
    let writer = JsonItemWriterBuilder::<Product>::new()
        .pretty_formatter(true)
        .from_path(&output_path);

    // Apply 10% discount
    let processor = DiscountProcessor::new(10.0);

    let step = StepBuilder::new("csv-to-json-discount")
        .chunk::<Product, Product>(10)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run()?;

    println!("  Applied 10% discount to all products");
    println!("  Output: {}", output_path.display());
    println!("  Duration: {:?}", result.duration);
    Ok(())
}

// =============================================================================
// Example 3: CSV with Custom Delimiter
// =============================================================================

/// Reads a semicolon-delimited CSV file.
fn example_custom_delimiter() -> Result<(), BatchError> {
    println!("\n=== Example 3: Custom Delimiter (semicolon) ===");

    // Semicolon-separated values (common in European locales)
    let csv_data = "\
id;name;price;category
1;Laptop;999.99;Electronics
2;Coffee Mug;12.99;Kitchen";

    let reader = CsvItemReaderBuilder::<Product>::new()
        .has_headers(true)
        .delimiter(b';')
        .from_reader(csv_data.as_bytes());

    let output_path = temp_dir().join("products_semicolon.csv");
    let writer = CsvItemWriterBuilder::<Product>::new()
        .has_headers(true)
        .delimiter(b';')
        .from_path(&output_path);

    let step = StepBuilder::new("semicolon-csv")
        .chunk::<Product, Product>(10)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    job.run()?;

    println!("  Processed semicolon-delimited CSV");
    println!("  Output: {}", output_path.display());
    Ok(())
}

// =============================================================================
// Example 4: Fault Tolerance with Skip Limit
// =============================================================================

/// Demonstrates error handling with malformed records.
fn example_fault_tolerance() -> Result<(), BatchError> {
    println!("\n=== Example 4: Fault Tolerance ===");

    // Note: Third row has invalid price "bad_price"
    let csv_data = "\
id,name,price,category
1,Laptop,999.99,Electronics
2,Coffee Mug,12.99,Kitchen
3,Invalid Item,bad_price,Error
4,Notebook,5.99,Office";

    let reader = CsvItemReaderBuilder::<Product>::new()
        .has_headers(true)
        .from_reader(csv_data.as_bytes());

    let output_path = temp_dir().join("products_valid.json");
    let writer = JsonItemWriterBuilder::<Product>::new().from_path(&output_path);

    let step = StepBuilder::new("fault-tolerant-csv")
        .chunk::<Product, Product>(2)
        .reader(&reader)
        .writer(&writer)
        .skip_limit(1) // Allow up to 1 read error
        .build();

    let job = JobBuilder::new().start(&step).build();
    job.run()?;

    // Verify the error was skipped
    let step_exec = job.get_step_execution("fault-tolerant-csv").unwrap();
    println!("  Read count: {}", step_exec.read_count);
    println!("  Write count: {}", step_exec.write_count);
    println!("  Read errors (skipped): {}", step_exec.read_error_count);
    println!("  Output: {}", output_path.display());

    Ok(())
}

// =============================================================================
// Main
// =============================================================================

fn main() -> Result<(), BatchError> {
    println!("CSV Processing Examples");
    println!("=======================\n");

    example_csv_to_csv()?;
    example_csv_to_json_with_processor()?;
    example_custom_delimiter()?;
    example_fault_tolerance()?;

    println!("\n✓ All CSV examples completed successfully!");
    Ok(())
}
