//! # JSON Processing Examples
//!
//! Demonstrates reading and writing JSON files with Spring Batch RS.
//!
//! ## Features Demonstrated
//! - Reading JSON arrays from files and in-memory sources
//! - Writing JSON with pretty formatting
//! - Converting JSON to other formats (CSV)
//! - Streaming large JSON files
//!
//! ## Run
//! ```bash
//! cargo run --example json_processing --features json,csv,logger
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
        csv::csv_writer::CsvItemWriterBuilder, json::json_reader::JsonItemReaderBuilder,
        json::json_writer::JsonItemWriterBuilder, logger::LoggerWriterBuilder,
    },
};
use std::env::temp_dir;
use std::io::Cursor;

// =============================================================================
// Data Structures
// =============================================================================

/// An order record for JSON processing.
#[derive(Debug, Deserialize, Serialize)]
struct Order {
    id: u64,
    customer: String,
    total: f64,
    status: String,
}

/// A simplified order for CSV export.
#[derive(Debug, Serialize)]
struct OrderSummary {
    order_id: u64,
    customer_name: String,
    amount: f64,
}

/// Processor that converts Order to OrderSummary.
struct OrderSummaryProcessor;

impl ItemProcessor<Order, OrderSummary> for OrderSummaryProcessor {
    fn process(&self, item: Order) -> Result<Option<OrderSummary>, BatchError> {
        Ok(Some(OrderSummary {
            order_id: item.id,
            customer_name: item.customer,
            amount: item.total,
        }))
    }
}

/// Processor that filters orders by status and applies tax.
struct CompletedOrderProcessor {
    tax_rate: f64,
}

impl CompletedOrderProcessor {
    fn new(tax_rate: f64) -> Self {
        Self { tax_rate }
    }
}

impl ItemProcessor<Order, Order> for CompletedOrderProcessor {
    fn process(&self, item: Order) -> Result<Option<Order>, BatchError> {
        // Apply tax to completed orders
        let total = if item.status == "completed" {
            item.total * (1.0 + self.tax_rate)
        } else {
            item.total
        };

        Ok(Some(Order {
            id: item.id,
            customer: item.customer,
            total,
            status: item.status,
        }))
    }
}

// =============================================================================
// Example 1: Read JSON Array
// =============================================================================

/// Reads a JSON array and logs each item.
fn example_read_json_array() -> Result<(), BatchError> {
    println!("=== Example 1: Read JSON Array ===");

    let json_data = r#"[
        {"id": 1, "customer": "Alice", "total": 99.99, "status": "completed"},
        {"id": 2, "customer": "Bob", "total": 149.50, "status": "pending"},
        {"id": 3, "customer": "Charlie", "total": 75.00, "status": "completed"}
    ]"#;

    let reader = JsonItemReaderBuilder::<Order>::new().from_reader(Cursor::new(json_data));

    let writer = LoggerWriterBuilder::<Order>::new().build();

    let step = StepBuilder::new("read-json-array")
        .chunk::<Order, Order>(2)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run()?;

    let step_exec = job.get_step_execution("read-json-array").unwrap();
    println!("  Items read: {}", step_exec.read_count);
    println!("  Duration: {:?}", result.duration);
    Ok(())
}

// =============================================================================
// Example 2: JSON to JSON with Transformation
// =============================================================================

/// Reads JSON, applies tax to completed orders, writes back to JSON.
fn example_json_transformation() -> Result<(), BatchError> {
    println!("\n=== Example 2: JSON Transformation ===");

    let json_data = r#"[
        {"id": 1, "customer": "Alice", "total": 100.00, "status": "completed"},
        {"id": 2, "customer": "Bob", "total": 200.00, "status": "pending"},
        {"id": 3, "customer": "Charlie", "total": 150.00, "status": "completed"}
    ]"#;

    let reader = JsonItemReaderBuilder::<Order>::new().from_reader(Cursor::new(json_data));

    let output_path = temp_dir().join("orders_with_tax.json");
    let writer = JsonItemWriterBuilder::<Order>::new()
        .pretty_formatter(true)
        .from_path(&output_path);

    // Apply 8% tax to completed orders
    let processor = CompletedOrderProcessor::new(0.08);

    let step = StepBuilder::new("apply-tax")
        .chunk::<Order, Order>(10)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    job.run()?;

    println!("  Applied 8% tax to completed orders");
    println!("  Output: {}", output_path.display());
    Ok(())
}

// =============================================================================
// Example 3: JSON to CSV Export
// =============================================================================

/// Converts JSON orders to a CSV summary report.
fn example_json_to_csv() -> Result<(), BatchError> {
    println!("\n=== Example 3: JSON to CSV Export ===");

    let json_data = r#"[
        {"id": 1001, "customer": "Alice Johnson", "total": 299.99, "status": "completed"},
        {"id": 1002, "customer": "Bob Smith", "total": 149.50, "status": "completed"},
        {"id": 1003, "customer": "Charlie Brown", "total": 75.00, "status": "pending"}
    ]"#;

    let reader = JsonItemReaderBuilder::<Order>::new().from_reader(Cursor::new(json_data));

    let output_path = temp_dir().join("order_summary.csv");
    let writer = CsvItemWriterBuilder::<OrderSummary>::new()
        .has_headers(true)
        .from_path(&output_path);

    let processor = OrderSummaryProcessor;

    let step = StepBuilder::new("json-to-csv")
        .chunk::<Order, OrderSummary>(10)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    job.run()?;

    println!("  Exported orders to CSV summary");
    println!("  Output: {}", output_path.display());
    Ok(())
}

// =============================================================================
// Example 4: Pretty vs Compact JSON Output
// =============================================================================

/// Demonstrates different JSON output formats.
fn example_json_formatting() -> Result<(), BatchError> {
    println!("\n=== Example 4: JSON Formatting Options ===");

    let json_data = r#"[
        {"id": 1, "customer": "Alice", "total": 99.99, "status": "completed"}
    ]"#;

    // Compact output (default)
    let reader1 = JsonItemReaderBuilder::<Order>::new().from_reader(Cursor::new(json_data));
    let compact_path = temp_dir().join("orders_compact.json");
    let writer1 = JsonItemWriterBuilder::<Order>::new()
        .pretty_formatter(false)
        .from_path(&compact_path);
    let step1 = StepBuilder::new("compact-json")
        .chunk::<Order, Order>(10)
        .reader(&reader1)
        .writer(&writer1)
        .build();

    // Pretty output
    let reader2 = JsonItemReaderBuilder::<Order>::new().from_reader(Cursor::new(json_data));
    let pretty_path = temp_dir().join("orders_pretty.json");
    let writer2 = JsonItemWriterBuilder::<Order>::new()
        .pretty_formatter(true)
        .from_path(&pretty_path);
    let step2 = StepBuilder::new("pretty-json")
        .chunk::<Order, Order>(10)
        .reader(&reader2)
        .writer(&writer2)
        .build();

    let job = JobBuilder::new().start(&step1).next(&step2).build();
    job.run()?;

    println!("  Compact output: {}", compact_path.display());
    println!("  Pretty output: {}", pretty_path.display());
    Ok(())
}

// =============================================================================
// Main
// =============================================================================

fn main() -> Result<(), BatchError> {
    env_logger::init();

    println!("JSON Processing Examples");
    println!("========================\n");

    example_read_json_array()?;
    example_json_transformation()?;
    example_json_to_csv()?;
    example_json_formatting()?;

    println!("\n✓ All JSON examples completed successfully!");
    Ok(())
}
