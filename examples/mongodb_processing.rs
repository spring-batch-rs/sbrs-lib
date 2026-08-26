//! # MongoDB Processing Examples
//!
//! Demonstrates reading from and writing to MongoDB with Spring Batch RS.
//!
//! **Note**: These examples require a running MongoDB instance at `localhost:27017`.
//!
//! ## Features Demonstrated
//! - Reading from MongoDB collections with filters
//! - Writing to MongoDB collections
//! - Pagination for large datasets
//! - Converting MongoDB data to CSV/JSON
//!
//! ## Prerequisites
//! ```bash
//! # Start MongoDB locally (using Docker)
//! docker run -d -p 27017:27017 --name mongodb mongo:latest
//! ```
//!
//! ## Run
//! ```bash
//! cargo run --example mongodb_processing --features mongodb,csv,json
//! ```

use mongodb::{
    bson::{doc, oid::ObjectId},
    sync::{Client, Collection},
};
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
        mongodb::mongodb_reader::{MongodbItemReaderBuilder, WithObjectId},
        mongodb::mongodb_writer::MongodbItemWriterBuilder,
    },
};
use std::env::temp_dir;

// =============================================================================
// Data Structures
// =============================================================================

/// A book document for MongoDB operations.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct Book {
    #[serde(rename = "_id", skip_serializing_if = "Option::is_none")]
    id: Option<ObjectId>,
    #[serde(rename = "oid")]
    object_id: ObjectId,
    title: String,
    author: String,
    year: i32,
    genre: String,
}

impl WithObjectId for Book {
    fn get_id(&self) -> ObjectId {
        self.object_id
    }
}

/// A simplified book record for CSV export (without ObjectId).
#[derive(Debug, Serialize)]
struct BookCsv {
    title: String,
    author: String,
    year: i32,
    genre: String,
}

/// Input record for importing books from CSV.
#[derive(Debug, Deserialize)]
struct BookInput {
    title: String,
    author: String,
    year: i32,
    genre: String,
}

/// Processor that converts Book to BookCsv for export.
struct BookToCsvProcessor;

impl ItemProcessor<Book, BookCsv> for BookToCsvProcessor {
    fn process(&self, item: Book) -> Result<Option<BookCsv>, BatchError> {
        Ok(Some(BookCsv {
            title: item.title,
            author: item.author,
            year: item.year,
            genre: item.genre,
        }))
    }
}

/// Processor that converts BookInput to Book for import.
struct BookFromCsvProcessor;

impl ItemProcessor<BookInput, Book> for BookFromCsvProcessor {
    fn process(&self, item: BookInput) -> Result<Option<Book>, BatchError> {
        let oid = ObjectId::new();
        Ok(Some(Book {
            id: Some(oid),
            object_id: oid,
            title: item.title,
            author: item.author,
            year: item.year,
            genre: item.genre,
        }))
    }
}

// =============================================================================
// Database Setup
// =============================================================================

/// Sets up the MongoDB database with sample data.
fn setup_database(collection: &Collection<Book>) -> Result<(), BatchError> {
    // Clear existing data
    collection
        .delete_many(doc! {})
        .run()
        .map_err(|e| BatchError::ItemWriter(e.to_string()))?;

    // Insert sample books
    let books = vec![
        create_book(
            "The Rust Programming Language",
            "Steve Klabnik",
            2019,
            "Programming",
        ),
        create_book("Programming Rust", "Jim Blandy", 2021, "Programming"),
        create_book("Rust in Action", "Tim McNamara", 2021, "Programming"),
        create_book(
            "Zero To Production In Rust",
            "Luca Palmieri",
            2022,
            "Programming",
        ),
        create_book("1984", "George Orwell", 1949, "Fiction"),
        create_book("Brave New World", "Aldous Huxley", 1932, "Fiction"),
    ];

    collection
        .insert_many(books)
        .run()
        .map_err(|e| BatchError::ItemWriter(e.to_string()))?;

    Ok(())
}

fn create_book(title: &str, author: &str, year: i32, genre: &str) -> Book {
    let oid = ObjectId::new();
    Book {
        id: Some(oid),
        object_id: oid,
        title: title.to_string(),
        author: author.to_string(),
        year,
        genre: genre.to_string(),
    }
}

// =============================================================================
// Example 1: Read All Documents
// =============================================================================

/// Reads all books from MongoDB and exports to JSON.
fn example_read_all_to_json(collection: &Collection<Book>) -> Result<(), BatchError> {
    println!("=== Example 1: Read All to JSON ===");

    let reader = MongodbItemReaderBuilder::new()
        .collection(collection)
        .page_size(10)
        .build();

    let output_path = temp_dir().join("all_books.json");
    let writer = JsonItemWriterBuilder::<Book>::new()
        .pretty_formatter(true)
        .from_path(&output_path);

    let step = StepBuilder::new("read-all-books")
        .chunk::<Book, Book>(5)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run()?;

    let step_exec = job.get_step_execution("read-all-books").unwrap();
    println!("  Books read: {}", step_exec.read_count);
    println!("  Output: {}", output_path.display());
    println!("  Duration: {:?}", result.duration);
    Ok(())
}

// =============================================================================
// Example 2: Read with Filter
// =============================================================================

/// Reads books filtered by genre and exports to CSV.
fn example_read_filtered_to_csv(collection: &Collection<Book>) -> Result<(), BatchError> {
    println!("\n=== Example 2: Read Filtered to CSV ===");

    let reader = MongodbItemReaderBuilder::new()
        .collection(collection)
        .filter(doc! { "genre": "Programming" })
        .page_size(10)
        .build();

    let output_path = temp_dir().join("programming_books.csv");
    let writer = CsvItemWriterBuilder::<BookCsv>::new()
        .has_headers(true)
        .from_path(&output_path);

    let processor = BookToCsvProcessor;

    let step = StepBuilder::new("read-programming-books")
        .chunk::<Book, BookCsv>(5)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    job.run()?;

    println!("  Exported programming books to CSV");
    println!("  Output: {}", output_path.display());
    Ok(())
}

// =============================================================================
// Example 3: Import from CSV
// =============================================================================

/// Imports books from CSV into MongoDB.
fn example_import_from_csv(collection: &Collection<Book>) -> Result<(), BatchError> {
    println!("\n=== Example 3: Import from CSV ===");

    let csv_data = "\
title,author,year,genre
Clean Code,Robert Martin,2008,Programming
Design Patterns,Gang of Four,1994,Programming
The Pragmatic Programmer,David Thomas,2019,Programming";

    let reader = CsvItemReaderBuilder::<BookInput>::new()
        .has_headers(true)
        .from_reader(csv_data.as_bytes());

    let writer = MongodbItemWriterBuilder::new()
        .collection(collection)
        .build();

    let processor = BookFromCsvProcessor;

    let step = StepBuilder::new("import-books")
        .chunk::<BookInput, Book>(2)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run()?;

    let step_exec = job.get_step_execution("import-books").unwrap();
    println!("  Books imported: {}", step_exec.write_count);
    println!("  Duration: {:?}", result.duration);
    Ok(())
}

// =============================================================================
// Example 4: Read with Year Filter
// =============================================================================

/// Reads books published after a certain year.
fn example_read_recent_books(collection: &Collection<Book>) -> Result<(), BatchError> {
    println!("\n=== Example 4: Read Recent Books (2020+) ===");

    let reader = MongodbItemReaderBuilder::new()
        .collection(collection)
        .filter(doc! { "year": { "$gte": 2020 } })
        .page_size(10)
        .build();

    let output_path = temp_dir().join("recent_books.json");
    let writer = JsonItemWriterBuilder::<Book>::new()
        .pretty_formatter(true)
        .from_path(&output_path);

    let step = StepBuilder::new("read-recent-books")
        .chunk::<Book, Book>(5)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run()?;

    let step_exec = job.get_step_execution("read-recent-books").unwrap();
    println!("  Recent books found: {}", step_exec.read_count);
    println!("  Output: {}", output_path.display());
    println!("  Duration: {:?}", result.duration);
    Ok(())
}

// =============================================================================
// Main
// =============================================================================

fn main() -> Result<(), BatchError> {
    println!("MongoDB Processing Examples");
    println!("===========================\n");
    println!("Note: Requires MongoDB running at localhost:27017\n");

    // Connect to MongoDB
    let client = Client::with_uri_str("mongodb://localhost:27017")
        .map_err(|e| BatchError::ItemReader(format!("Failed to connect to MongoDB: {}", e)))?;

    let db = client.database("spring_batch_example");
    let collection: Collection<Book> = db.collection("books");

    // Setup database with sample data
    setup_database(&collection)?;
    println!("Database initialized with sample data.\n");

    // Run examples
    example_read_all_to_json(&collection)?;
    example_read_filtered_to_csv(&collection)?;
    example_import_from_csv(&collection)?;
    example_read_recent_books(&collection)?;

    println!("\n✓ All MongoDB examples completed successfully!");
    Ok(())
}
