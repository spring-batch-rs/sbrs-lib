//! # XML Processing Examples
//!
//! Demonstrates reading and writing XML files with Spring Batch RS.
//!
//! ## Features Demonstrated
//! - Reading XML elements by tag name
//! - Writing XML with custom root and item tags
//! - Handling XML attributes
//! - Converting XML to JSON and CSV
//!
//! ## Run
//! ```bash
//! cargo run --example xml_processing --features xml,json,csv
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
        csv::csv_writer::CsvItemWriterBuilder, json::json_writer::JsonItemWriterBuilder,
        xml::xml_reader::XmlItemReaderBuilder, xml::xml_writer::XmlItemWriterBuilder,
    },
};
use std::env::temp_dir;
use std::io::Cursor;

// =============================================================================
// Data Structures
// =============================================================================

/// A book record from XML.
/// XML attributes are handled with `@` prefix in serde rename.
#[derive(Debug, Deserialize, Serialize)]
struct Book {
    #[serde(rename = "@id")]
    id: u32,
    title: String,
    author: String,
    price: f64,
}

/// A person record for XML processing.
#[derive(Debug, Deserialize, Serialize)]
struct Person {
    name: String,
    email: String,
    age: u32,
}

/// A house record with nested structure.
#[derive(Debug, Deserialize, Serialize)]
struct House {
    #[serde(rename = "@id")]
    id: u32,
    address: String,
    city: String,
    bedrooms: u32,
    price: f64,
}

/// CSV-friendly house record without XML attributes.
#[derive(Debug, Serialize)]
struct HouseCsv {
    id: u32,
    address: String,
    city: String,
    bedrooms: u32,
    price: f64,
}

/// Processor to convert House to HouseCsv.
struct HouseToCsvProcessor;

impl ItemProcessor<House, HouseCsv> for HouseToCsvProcessor {
    fn process(&self, item: House) -> Result<Option<HouseCsv>, BatchError> {
        Ok(Some(HouseCsv {
            id: item.id,
            address: item.address,
            city: item.city,
            bedrooms: item.bedrooms,
            price: item.price,
        }))
    }
}

// =============================================================================
// Example 1: Read XML Elements
// =============================================================================

/// Reads XML elements by tag name and converts to JSON.
fn example_read_xml_to_json() -> Result<(), BatchError> {
    println!("=== Example 1: XML to JSON ===");

    let xml_data = r#"<?xml version="1.0" encoding="UTF-8"?>
<catalog>
    <book id="1">
        <title>The Rust Programming Language</title>
        <author>Steve Klabnik</author>
        <price>39.99</price>
    </book>
    <book id="2">
        <title>Programming Rust</title>
        <author>Jim Blandy</author>
        <price>49.99</price>
    </book>
    <book id="3">
        <title>Rust in Action</title>
        <author>Tim McNamara</author>
        <price>44.99</price>
    </book>
</catalog>"#;

    let reader = XmlItemReaderBuilder::<Book>::new()
        .tag("book")
        .from_reader(Cursor::new(xml_data));

    let output_path = temp_dir().join("books.json");
    let writer = JsonItemWriterBuilder::<Book>::new()
        .pretty_formatter(true)
        .from_path(&output_path);

    let step = StepBuilder::new("xml-to-json")
        .chunk::<Book, Book>(10)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run()?;

    let step_exec = job.get_step_execution("xml-to-json").unwrap();
    println!("  Books read: {}", step_exec.read_count);
    println!("  Output: {}", output_path.display());
    println!("  Duration: {:?}", result.duration);
    Ok(())
}

// =============================================================================
// Example 2: Write XML from Data
// =============================================================================

/// Creates an XML file from in-memory data.
fn example_write_xml() -> Result<(), BatchError> {
    println!("\n=== Example 2: Create XML File ===");

    // Create in-memory data using a simple reader simulation
    let json_data = r#"[
        {"name": "Alice", "email": "alice@example.com", "age": 30},
        {"name": "Bob", "email": "bob@example.com", "age": 25},
        {"name": "Charlie", "email": "charlie@example.com", "age": 35}
    ]"#;

    let reader = spring_batch_rs::item::json::json_reader::JsonItemReaderBuilder::<Person>::new()
        .from_reader(Cursor::new(json_data));

    let output_path = temp_dir().join("people.xml");
    let writer = XmlItemWriterBuilder::<Person>::new()
        .root_tag("people")
        .item_tag("person")
        .from_path(&output_path)?;

    let step = StepBuilder::new("create-xml")
        .chunk::<Person, Person>(10)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    job.run()?;

    println!("  Created XML with <people> root and <person> items");
    println!("  Output: {}", output_path.display());
    Ok(())
}

// =============================================================================
// Example 3: XML to CSV with Transformation
// =============================================================================

/// Converts XML data to CSV with data transformation.
fn example_xml_to_csv() -> Result<(), BatchError> {
    println!("\n=== Example 3: XML to CSV ===");

    let xml_data = r#"<?xml version="1.0" encoding="UTF-8"?>
<listings>
    <house id="101">
        <address>123 Main St</address>
        <city>Springfield</city>
        <bedrooms>3</bedrooms>
        <price>250000</price>
    </house>
    <house id="102">
        <address>456 Oak Ave</address>
        <city>Shelbyville</city>
        <bedrooms>4</bedrooms>
        <price>320000</price>
    </house>
    <house id="103">
        <address>789 Pine Rd</address>
        <city>Capital City</city>
        <bedrooms>2</bedrooms>
        <price>180000</price>
    </house>
</listings>"#;

    let reader = XmlItemReaderBuilder::<House>::new()
        .tag("house")
        .from_reader(Cursor::new(xml_data));

    let output_path = temp_dir().join("houses.csv");
    let writer = CsvItemWriterBuilder::<HouseCsv>::new()
        .has_headers(true)
        .from_path(&output_path);

    let processor = HouseToCsvProcessor;

    let step = StepBuilder::new("xml-to-csv")
        .chunk::<House, HouseCsv>(10)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    job.run()?;

    println!("  Converted house listings to CSV");
    println!("  Output: {}", output_path.display());
    Ok(())
}

// =============================================================================
// Example 4: XML to XML Transformation
// =============================================================================

/// Reads XML and writes to a different XML structure.
fn example_xml_to_xml() -> Result<(), BatchError> {
    println!("\n=== Example 4: XML to XML ===");

    let xml_data = r#"<?xml version="1.0" encoding="UTF-8"?>
<library>
    <book id="1">
        <title>Clean Code</title>
        <author>Robert Martin</author>
        <price>34.99</price>
    </book>
    <book id="2">
        <title>Design Patterns</title>
        <author>Gang of Four</author>
        <price>54.99</price>
    </book>
</library>"#;

    let reader = XmlItemReaderBuilder::<Book>::new()
        .tag("book")
        .from_reader(Cursor::new(xml_data));

    let output_path = temp_dir().join("inventory.xml");
    let writer = XmlItemWriterBuilder::<Book>::new()
        .root_tag("inventory")
        .item_tag("item")
        .from_path(&output_path)?;

    let step = StepBuilder::new("xml-to-xml")
        .chunk::<Book, Book>(10)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    job.run()?;

    println!("  Transformed <library>/<book> to <inventory>/<item>");
    println!("  Output: {}", output_path.display());
    Ok(())
}

// =============================================================================
// Main
// =============================================================================

fn main() -> Result<(), BatchError> {
    println!("XML Processing Examples");
    println!("=======================\n");

    example_read_xml_to_json()?;
    example_write_xml()?;
    example_xml_to_csv()?;
    example_xml_to_xml()?;

    println!("\n✓ All XML examples completed successfully!");
    Ok(())
}
