use anyhow::Result;
use std::io::Read;
use testcontainers_modules::{mongo, testcontainers::runners::SyncRunner};

use mongodb::{
    bson::{doc, oid::ObjectId},
    sync::Client,
};
use serde::{Deserialize, Serialize};
use spring_batch_rs::{
    core::{
        item::{ItemProcessor, ItemProcessorResult},
        job::{Job, JobBuilder},
        step::{StepBuilder, StepStatus},
    },
    item::{
        csv::{csv_reader::CsvItemReaderBuilder, csv_writer::CsvItemWriterBuilder},
        mongodb::{
            mongodb_reader::{MongodbItemReaderBuilder, WithObjectId},
            mongodb_writer::MongodbItemWriterBuilder,
        },
    },
};
use tempfile::NamedTempFile;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Book {
    #[serde(rename = "oid")]
    id: ObjectId,
    title: String,
    author: String,
}

impl WithObjectId for Book {
    fn get_id(&self) -> ObjectId {
        self.id
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct FormattedBook {
    title: String,
    author: String,
}

#[derive(Default)]
struct FormatBookProcessor {}

impl ItemProcessor<Book, FormattedBook> for FormatBookProcessor {
    fn process(&self, item: Book) -> ItemProcessorResult<FormattedBook> {
        let book = FormattedBook {
            title: item.title.replace(" ", "_").to_uppercase(),
            author: item.author.replace(" ", "_").to_uppercase(),
        };

        Ok(Some(book))
    }
}

#[test]
fn read_items_from_database() -> Result<()> {
    let container = mongo::Mongo::default().start().unwrap();
    let host_ip = container.get_host().unwrap();
    let host_port = container.get_host_port_ipv4(27017).unwrap();

    let url = format!("mongodb://{host_ip}:{host_port}/");

    let client: Client = Client::with_uri_str(&url).unwrap();

    let db = client.database("test");

    let book_collection = db.collection::<Book>("books");

    let books = vec![
        Book {
            id: ObjectId::new(),
            title: "01 The Grapes of Wrath".to_string(),
            author: "Harper Steinbeck".to_string(),
        },
        Book {
            id: ObjectId::new(),
            title: "02 To Kill a Mockingbird".to_string(),
            author: "Harper Lee".to_string(),
        },
        Book {
            id: ObjectId::new(),
            title: "03 To Kill a Mockingbird".to_string(),
            author: "Harper Lee".to_string(),
        },
        Book {
            id: ObjectId::new(),
            title: "04 To Kill a Mockingbird".to_string(),
            author: "Harper Lee".to_string(),
        },
        Book {
            id: ObjectId::new(),
            title: "05 To Kill a Mockingbird".to_string(),
            author: "Harper Lee".to_string(),
        },
        Book {
            id: ObjectId::new(),
            title: "06 To Kill a Mockingbird".to_string(),
            author: "Harper Lee".to_string(),
        },
        Book {
            id: ObjectId::new(),
            title: "07 To Kill a Mockingbird".to_string(),
            author: "Harper Lee".to_string(),
        },
        Book {
            id: ObjectId::new(),
            title: "08 To Kill a Mockingbird".to_string(),
            author: "Harper Lee".to_string(),
        },
        Book {
            id: ObjectId::new(),
            title: "09 To Kill a Mockingbird".to_string(),
            author: "Harper Lee".to_string(),
        },
        Book {
            id: ObjectId::new(),
            title: "10 To Kill a Mockingbird".to_string(),
            author: "Harper Lee".to_string(),
        },
        Book {
            id: ObjectId::new(),
            title: "11 To Kill a Mockingbird".to_string(),
            author: "Harper Lee".to_string(),
        },
        Book {
            id: ObjectId::new(),
            title: "12 To Kill a Mockingbird".to_string(),
            author: "Harper Lee".to_string(),
        },
        Book {
            id: ObjectId::new(),
            title: "13 To Kill a Mockingbird".to_string(),
            author: "Harper Lee".to_string(),
        },
    ];

    let _ = book_collection.insert_many(books).run();

    let filter = doc! {"title": {"$regex": "To Kill"}};

    // Prepare reader
    let reader = MongodbItemReaderBuilder::new()
        .collection(&book_collection)
        .filter(filter)
        .page_size(3)
        .build();

    // Prepare processor
    let processor = FormatBookProcessor::default();

    // Prepare writer
    let tmpfile = NamedTempFile::new()?;

    let writer = CsvItemWriterBuilder::new().from_writer(tmpfile.as_file());

    let step = StepBuilder::new("test")
        .chunk(3)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run();

    let step_execution = job.get_step_execution("test").unwrap();

    assert!(result.is_ok());
    assert!(step_execution.status == StepStatus::Success);
    assert!(step_execution.read_count == 12);
    assert!(step_execution.write_count == 12);
    assert!(step_execution.read_error_count == 0);
    assert!(step_execution.write_error_count == 0);

    let mut tmpfile = tmpfile.reopen()?;
    let mut file_content = String::new();

    tmpfile
        .read_to_string(&mut file_content)
        .expect("Should have been able to read the file");

    assert_eq!(
        file_content,
        "02_TO_KILL_A_MOCKINGBIRD,HARPER_LEE
03_TO_KILL_A_MOCKINGBIRD,HARPER_LEE
04_TO_KILL_A_MOCKINGBIRD,HARPER_LEE
05_TO_KILL_A_MOCKINGBIRD,HARPER_LEE
06_TO_KILL_A_MOCKINGBIRD,HARPER_LEE
07_TO_KILL_A_MOCKINGBIRD,HARPER_LEE
08_TO_KILL_A_MOCKINGBIRD,HARPER_LEE
09_TO_KILL_A_MOCKINGBIRD,HARPER_LEE
10_TO_KILL_A_MOCKINGBIRD,HARPER_LEE
11_TO_KILL_A_MOCKINGBIRD,HARPER_LEE
12_TO_KILL_A_MOCKINGBIRD,HARPER_LEE
13_TO_KILL_A_MOCKINGBIRD,HARPER_LEE
"
    );

    Ok(())
}

#[test]
fn write_items_to_database() -> Result<()> {
    let container = mongo::Mongo::default().start().unwrap();
    let host_ip = container.get_host().unwrap();
    let host_port = container.get_host_port_ipv4(27017).unwrap();

    let url = format!("mongodb://{host_ip}:{host_port}/");

    let client: Client = Client::with_uri_str(&url).unwrap();

    let db = client.database("test");

    let book_collection = db.collection::<FormattedBook>("books");

    // Prepare reader
    let csv = "title,author
            Shining,Stephen King
            UN SAC DE BILLES,JOSEPH JOFFO";

    let reader = CsvItemReaderBuilder::<FormattedBook>::new()
        .has_headers(true)
        .from_reader(csv.as_bytes());

    // Prepare writer
    let writer = MongodbItemWriterBuilder::new()
        .collection(&book_collection)
        .build();

    // Execute process
    let step = StepBuilder::new("test")
        .chunk::<FormattedBook, FormattedBook>(3)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();

    let result = job.run();
    assert!(result.is_ok());

    let step_execution = job.get_step_execution("test").unwrap();
    assert!(step_execution.status == StepStatus::Success);
    assert!(step_execution.read_count == 2);
    assert!(step_execution.write_count == 2);
    assert!(step_execution.read_error_count == 0);
    assert!(step_execution.write_error_count == 0);

    Ok(())
}
