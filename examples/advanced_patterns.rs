//! # Advanced Batch Processing Patterns
//!
//! Demonstrates advanced patterns and techniques with Spring Batch RS.
//!
//! ## Features Demonstrated
//! - Multi-step ETL pipelines
//! - Custom processors with business logic
//! - Conditional processing
//! - Error handling and skip policies
//! - Job execution monitoring
//! - Chaining readers and writers
//!
//! ## Run
//! ```bash
//! cargo run --example advanced_patterns --features csv,json,logger
//! ```

use serde::{Deserialize, Serialize};
use spring_batch_rs::{
    BatchError,
    core::{
        item::{ItemProcessor, ItemReader},
        job::{Job, JobBuilder},
        step::StepBuilder,
    },
    item::{
        csv::csv_reader::CsvItemReaderBuilder, csv::csv_writer::CsvItemWriterBuilder,
        json::json_reader::JsonItemReaderBuilder, json::json_writer::JsonItemWriterBuilder,
        logger::LoggerWriterBuilder,
    },
};
use std::{cell::RefCell, collections::VecDeque, env::temp_dir, fs::File};

// =============================================================================
// Data Structures
// =============================================================================

/// Raw transaction record from input.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct RawTransaction {
    id: u32,
    account: String,
    amount: f64,
    transaction_type: String,
    status: String,
}

/// Validated transaction after filtering.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct ValidTransaction {
    id: u32,
    account: String,
    amount: f64,
    transaction_type: String,
}

/// Enriched transaction with computed fields.
#[derive(Debug, Clone, Serialize)]
struct EnrichedTransaction {
    transaction_id: String,
    account_number: String,
    gross_amount: f64,
    fee: f64,
    net_amount: f64,
    category: String,
}

/// Summary report record.
#[derive(Debug, Clone, Serialize)]
struct TransactionSummary {
    account: String,
    total_credits: f64,
    total_debits: f64,
    net_balance: f64,
    transaction_count: u32,
}

// =============================================================================
// Custom Processors
// =============================================================================

/// Filters out cancelled/failed transactions and validates data.
struct ValidationProcessor;

impl ItemProcessor<RawTransaction, ValidTransaction> for ValidationProcessor {
    fn process(&self, item: RawTransaction) -> Result<Option<ValidTransaction>, BatchError> {
        // Business filtering: non-completed transactions are intentionally skipped,
        // not errors. Ok(None) signals the framework to count this as a filtered item
        // and not pass it to the writer — no fault tolerance triggered.
        if item.status != "completed" {
            return Ok(None);
        }

        // Data error: a non-positive amount is invalid data, not a business decision.
        // Err(...) signals a processing error and may trigger fault tolerance / skip logic.
        if item.amount <= 0.0 {
            return Err(BatchError::ItemProcessor(format!(
                "Invalid amount for transaction {}: {}",
                item.id, item.amount
            )));
        }

        Ok(Some(ValidTransaction {
            id: item.id,
            account: item.account,
            amount: item.amount,
            transaction_type: item.transaction_type,
        }))
    }
}

/// Enriches transactions with computed fields.
struct EnrichmentProcessor {
    fee_rate: f64,
}

impl EnrichmentProcessor {
    fn new(fee_rate: f64) -> Self {
        Self { fee_rate }
    }
}

impl ItemProcessor<ValidTransaction, EnrichedTransaction> for EnrichmentProcessor {
    fn process(&self, item: ValidTransaction) -> Result<Option<EnrichedTransaction>, BatchError> {
        let fee = if item.transaction_type == "credit" {
            0.0 // No fee for credits
        } else {
            item.amount * self.fee_rate
        };

        let category = match item.amount {
            a if a >= 10000.0 => "large",
            a if a >= 1000.0 => "medium",
            _ => "small",
        };

        Ok(Some(EnrichedTransaction {
            transaction_id: format!("TXN-{:06}", item.id),
            account_number: item.account,
            gross_amount: item.amount,
            fee,
            net_amount: item.amount - fee,
            category: category.to_string(),
        }))
    }
}

// =============================================================================
// Helper: In-Memory Reader
// =============================================================================

/// A simple in-memory reader for demonstration.
struct InMemoryReader<T> {
    items: RefCell<VecDeque<T>>,
}

impl<T: Clone> InMemoryReader<T> {
    fn new(items: Vec<T>) -> Self {
        Self {
            items: RefCell::new(items.into()),
        }
    }
}

impl<T: Clone> ItemReader<T> for InMemoryReader<T> {
    fn read(&self) -> Result<Option<T>, BatchError> {
        Ok(self.items.borrow_mut().pop_front())
    }
}

// =============================================================================
// Example 1: Multi-Step ETL Pipeline
// =============================================================================

/// Demonstrates a complete ETL pipeline: Extract -> Validate -> Enrich -> Load.
fn example_multi_step_etl() -> Result<(), BatchError> {
    println!("=== Example 1: Multi-Step ETL Pipeline ===");
    println!("  Pipeline: CSV -> Validate -> JSON -> Enrich -> CSV\n");

    // Input data with various statuses
    let raw_csv = "\
id,account,amount,transaction_type,status
1,ACC001,5000.00,debit,completed
2,ACC001,1500.00,credit,completed
3,ACC002,250.00,debit,cancelled
4,ACC002,10000.00,debit,completed
5,ACC003,750.00,credit,failed
6,ACC003,3000.00,debit,completed";

    // Step 1: Read CSV and validate (filter out non-completed)
    println!("  Step 1: Validating transactions...");

    let csv_reader = CsvItemReaderBuilder::<RawTransaction>::new()
        .has_headers(true)
        .from_reader(raw_csv.as_bytes());

    let intermediate_path = temp_dir().join("validated_transactions.json");
    let json_writer =
        JsonItemWriterBuilder::<ValidTransaction>::new().from_path(&intermediate_path);

    let validation_processor = ValidationProcessor;

    let validate_step = StepBuilder::new("validate-transactions")
        .chunk::<RawTransaction, ValidTransaction>(10)
        .reader(&csv_reader)
        .processor(&validation_processor)
        .writer(&json_writer)
        .skip_limit(10) // Allow skipping invalid records
        .build();

    // Step 2: Read validated JSON and enrich
    println!("  Step 2: Enriching transactions...");

    let json_file = File::open(&intermediate_path)
        .map_err(|e| BatchError::ItemReader(format!("Cannot open intermediate file: {}", e)))?;
    let json_reader = JsonItemReaderBuilder::<ValidTransaction>::new().from_reader(json_file);

    let output_path = temp_dir().join("enriched_transactions.csv");
    let csv_writer = CsvItemWriterBuilder::<EnrichedTransaction>::new()
        .has_headers(true)
        .from_path(&output_path);

    let enrichment_processor = EnrichmentProcessor::new(0.02); // 2% fee

    let enrich_step = StepBuilder::new("enrich-transactions")
        .chunk::<ValidTransaction, EnrichedTransaction>(10)
        .reader(&json_reader)
        .processor(&enrichment_processor)
        .writer(&csv_writer)
        .build();

    // Build and run the job
    let job = JobBuilder::new()
        .start(&validate_step)
        .next(&enrich_step)
        .build();

    let result = job.run()?;

    // Print execution summary
    let step1_exec = job.get_step_execution("validate-transactions").unwrap();
    let step2_exec = job.get_step_execution("enrich-transactions").unwrap();

    println!("\n  Results:");
    println!(
        "    Validation: {} read, {} written, {} skipped",
        step1_exec.read_count, step1_exec.write_count, step1_exec.read_error_count
    );
    println!(
        "    Enrichment: {} read, {} written",
        step2_exec.read_count, step2_exec.write_count
    );
    println!("    Total duration: {:?}", result.duration);
    println!("    Output: {}", output_path.display());

    Ok(())
}

// =============================================================================
// Example 2: Parallel Format Conversion
// =============================================================================

/// Demonstrates converting same data to multiple formats.
fn example_parallel_conversion() -> Result<(), BatchError> {
    println!("\n=== Example 2: Multi-Format Export ===");

    let transactions = vec![
        ValidTransaction {
            id: 1,
            account: "ACC001".to_string(),
            amount: 1000.0,
            transaction_type: "debit".to_string(),
        },
        ValidTransaction {
            id: 2,
            account: "ACC002".to_string(),
            amount: 2500.0,
            transaction_type: "credit".to_string(),
        },
        ValidTransaction {
            id: 3,
            account: "ACC003".to_string(),
            amount: 500.0,
            transaction_type: "debit".to_string(),
        },
    ];

    // Export to JSON
    let json_reader = InMemoryReader::new(transactions.clone());
    let json_path = temp_dir().join("transactions.json");
    let json_writer = JsonItemWriterBuilder::<ValidTransaction>::new()
        .pretty_formatter(true)
        .from_path(&json_path);

    let json_step = StepBuilder::new("export-json")
        .chunk::<ValidTransaction, ValidTransaction>(10)
        .reader(&json_reader)
        .writer(&json_writer)
        .build();

    // Export to CSV
    let csv_reader = InMemoryReader::new(transactions);
    let csv_path = temp_dir().join("transactions.csv");
    let csv_writer = CsvItemWriterBuilder::<ValidTransaction>::new()
        .has_headers(true)
        .from_path(&csv_path);

    let csv_step = StepBuilder::new("export-csv")
        .chunk::<ValidTransaction, ValidTransaction>(10)
        .reader(&csv_reader)
        .writer(&csv_writer)
        .build();

    // Run both exports
    let job = JobBuilder::new().start(&json_step).next(&csv_step).build();
    job.run()?;

    println!("  Exported to:");
    println!("    - {}", json_path.display());
    println!("    - {}", csv_path.display());

    Ok(())
}

// =============================================================================
// Example 3: Aggregation Pipeline
// =============================================================================

/// Demonstrates aggregating data from multiple records.
fn example_aggregation_pipeline() -> Result<(), BatchError> {
    println!("\n=== Example 3: Aggregation Pipeline ===");

    // Simulate reading transactions and computing summaries per account
    let transactions = vec![
        ValidTransaction {
            id: 1,
            account: "ACC001".to_string(),
            amount: 1000.0,
            transaction_type: "credit".to_string(),
        },
        ValidTransaction {
            id: 2,
            account: "ACC001".to_string(),
            amount: 500.0,
            transaction_type: "debit".to_string(),
        },
        ValidTransaction {
            id: 3,
            account: "ACC001".to_string(),
            amount: 200.0,
            transaction_type: "credit".to_string(),
        },
        ValidTransaction {
            id: 4,
            account: "ACC002".to_string(),
            amount: 3000.0,
            transaction_type: "credit".to_string(),
        },
        ValidTransaction {
            id: 5,
            account: "ACC002".to_string(),
            amount: 1500.0,
            transaction_type: "debit".to_string(),
        },
    ];

    // Compute summaries manually (in production, use a custom aggregating writer)
    use std::collections::HashMap;
    let mut accounts: HashMap<String, (f64, f64, u32)> = HashMap::new();

    for txn in &transactions {
        let entry = accounts.entry(txn.account.clone()).or_insert((0.0, 0.0, 0));
        if txn.transaction_type == "credit" {
            entry.0 += txn.amount;
        } else {
            entry.1 += txn.amount;
        }
        entry.2 += 1;
    }

    let summaries: Vec<TransactionSummary> = accounts
        .into_iter()
        .map(|(account, (credits, debits, count))| TransactionSummary {
            account,
            total_credits: credits,
            total_debits: debits,
            net_balance: credits - debits,
            transaction_count: count,
        })
        .collect();

    // Write summaries
    let reader = InMemoryReader::new(summaries);
    let output_path = temp_dir().join("account_summaries.csv");
    let writer = CsvItemWriterBuilder::<TransactionSummary>::new()
        .has_headers(true)
        .from_path(&output_path);

    let step = StepBuilder::new("write-summaries")
        .chunk::<TransactionSummary, TransactionSummary>(10)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    job.run()?;

    println!("  Aggregated {} accounts", 2);
    println!("  Output: {}", output_path.display());

    Ok(())
}

// =============================================================================
// Example 4: Error Handling and Monitoring
// =============================================================================

/// Demonstrates error handling, skip policies, and monitoring.
fn example_error_handling() -> Result<(), BatchError> {
    println!("\n=== Example 4: Error Handling and Monitoring ===");

    // Data with some invalid records
    let csv_data = "\
id,account,amount,transaction_type,status
1,ACC001,1000.00,debit,completed
2,ACC002,invalid_amount,credit,completed
3,ACC003,2000.00,debit,completed
4,ACC004,-500.00,debit,completed
5,ACC005,3000.00,credit,completed";

    let reader = CsvItemReaderBuilder::<RawTransaction>::new()
        .has_headers(true)
        .from_reader(csv_data.as_bytes());

    let writer = LoggerWriterBuilder::<ValidTransaction>::new().build();
    let processor = ValidationProcessor;

    let step = StepBuilder::new("error-handling-step")
        .chunk::<RawTransaction, ValidTransaction>(2)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .skip_limit(5) // Allow up to 5 errors
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run()?;

    let step_exec = job.get_step_execution("error-handling-step").unwrap();

    println!("\n  Execution Summary:");
    println!("    Status: {:?}", step_exec.status);
    println!("    Read count: {}", step_exec.read_count);
    println!("    Write count: {}", step_exec.write_count);
    println!("    Read errors: {}", step_exec.read_error_count);
    println!("    Process errors: {}", step_exec.process_error_count);
    println!("    Duration: {:?}", result.duration);

    Ok(())
}

// =============================================================================
// Main
// =============================================================================

fn main() -> Result<(), BatchError> {
    env_logger::init();

    println!("Advanced Batch Processing Patterns");
    println!("==================================\n");

    example_multi_step_etl()?;
    example_parallel_conversion()?;
    example_aggregation_pipeline()?;
    example_error_handling()?;

    println!("\n✓ All advanced pattern examples completed successfully!");

    Ok(())
}
