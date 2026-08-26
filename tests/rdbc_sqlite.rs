mod helpers;

use std::{io::Read, path::Path};

use helpers::{
    common::{DEFAULT_CHUNK_SIZE, EXPECTED_PERSON_COUNT, EXPECTED_PERSON_CSV, SAMPLE_CARS_CSV},
    sqlite_helpers::{CREATE_CARS_TABLE_SQL, Car, SELECT_ALL_CARS_SQL},
};
use serde::{Deserialize, Serialize};
use spring_batch_rs::{
    core::{
        item::ItemReader,
        job::{Job, JobBuilder},
        step::{StepBuilder, StepStatus},
    },
    item::{
        csv::{csv_reader::CsvItemReaderBuilder, csv_writer::CsvItemWriterBuilder},
        rdbc::{RdbcItemReaderBuilder, RdbcItemWriterBuilder, SelectBuilder},
    },
};
use sqlx::{
    FromRow, Sqlite, SqlitePool,
    migrate::{MigrateDatabase, Migrator},
};
use tempfile::NamedTempFile;

#[derive(Serialize, Deserialize, Clone, FromRow)]
struct Person {
    id: Option<i32>,
    first_name: String,
    last_name: String,
}

#[tokio::test(flavor = "multi_thread")]
async fn read_items_from_database() -> Result<(), sqlx::Error> {
    // Prepare database
    let database_file = NamedTempFile::new()?;
    let database_path = database_file.path().to_str().unwrap();
    let connection_uri = format!("sqlite://{}", database_path);

    if !Sqlite::database_exists(&connection_uri)
        .await
        .unwrap_or(false)
    {
        Sqlite::create_database(&connection_uri).await?;
    }

    let pool = SqlitePool::connect(&connection_uri).await?;
    let migrator = Migrator::new(Path::new("tests/migrations/sqlite")).await?;
    migrator.run(&pool).await?;

    // Prepare reader using unified builder
    let query = "SELECT * from person";
    let reader = RdbcItemReaderBuilder::new()
        .sqlite(pool.clone())
        .query(query)
        .with_page_size(5)
        .build_sqlite();

    // Prepare writer
    let tmpfile = NamedTempFile::new()?;
    let writer = CsvItemWriterBuilder::new()
        .has_headers(true)
        .from_writer(tmpfile.as_file());

    // Execute process
    let step = StepBuilder::new("test")
        .chunk::<Person, Person>(DEFAULT_CHUNK_SIZE)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run();
    assert!(result.is_ok());

    let step_execution = job.get_step_execution("test").unwrap();

    assert_eq!(step_execution.status, StepStatus::Success);
    assert_eq!(step_execution.read_count, EXPECTED_PERSON_COUNT);
    assert_eq!(step_execution.write_count, EXPECTED_PERSON_COUNT);
    assert_eq!(step_execution.read_error_count, 0);
    assert_eq!(step_execution.write_error_count, 0);

    let mut tmpfile = tmpfile.reopen()?;
    let mut file_content = String::new();

    tmpfile
        .read_to_string(&mut file_content)
        .expect("Should have been able to read the file");

    assert_eq!(file_content, EXPECTED_PERSON_CSV);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn write_items_to_database() -> Result<(), sqlx::Error> {
    // Prepare reader
    let reader = CsvItemReaderBuilder::<Car>::new()
        .has_headers(true)
        .from_reader(SAMPLE_CARS_CSV.as_bytes());

    // Prepare writer
    let database_file = NamedTempFile::new()?;
    let database_path = database_file.path().to_str().unwrap();
    let connection_uri = format!("sqlite://{}", database_path);

    if !Sqlite::database_exists(&connection_uri)
        .await
        .unwrap_or(false)
    {
        Sqlite::create_database(&connection_uri).await?;
    }

    let pool = SqlitePool::connect(&connection_uri).await?;

    // Create table
    sqlx::query(CREATE_CARS_TABLE_SQL).execute(&pool).await?;

    let writer = RdbcItemWriterBuilder::<Car>::new()
        .sqlite(&pool)
        .table("cars")
        .column("year", |c: &Car| c.year.into())
        .column("make", |c: &Car| c.make.as_str().into())
        .column("model", |c: &Car| c.model.as_str().into())
        .column("description", |c: &Car| c.description.as_str().into())
        .build_sqlite();

    // Execute process
    let step = StepBuilder::new("test")
        .chunk::<Car, Car>(DEFAULT_CHUNK_SIZE)
        .reader(&reader)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run();
    assert!(result.is_ok());

    let step_execution = job.get_step_execution("test").unwrap();

    assert_eq!(step_execution.status, StepStatus::Success);
    assert_eq!(
        step_execution.read_count,
        helpers::common::EXPECTED_CAR_COUNT
    );
    assert_eq!(
        step_execution.write_count,
        helpers::common::EXPECTED_CAR_COUNT
    );
    assert_eq!(step_execution.read_error_count, 0);
    assert_eq!(step_execution.write_error_count, 0);

    let car_results = sqlx::query_as::<_, Car>(SELECT_ALL_CARS_SQL)
        .fetch_all(&pool)
        .await?;

    assert_eq!(car_results.len(), helpers::common::EXPECTED_CAR_COUNT);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn should_read_all_pages_via_select_builder_with_keyset() {
    let pool = SqlitePool::connect("sqlite::memory:")
        .await
        .expect("in-memory SQLite pool should open");

    sqlx::query("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        .execute(&pool)
        .await
        .expect("CREATE TABLE should succeed");

    for i in 1..=5_i32 {
        sqlx::query("INSERT INTO items (id, name) VALUES (?, ?)")
            .bind(i)
            .bind(format!("item{}", i))
            .execute(&pool)
            .await
            .expect("INSERT should succeed");
    }

    #[derive(Clone, sqlx::FromRow)]
    struct Item {
        id: i32,
        name: String,
    }

    let reader = RdbcItemReaderBuilder::<Item>::new()
        .sqlite(pool)
        .select(
            SelectBuilder::from("items")
                .columns(&["id", "name"])
                .order_by_keyset("id", |i: &Item| i.id.to_string()),
        )
        .with_page_size(2)
        .build_sqlite();

    let mut names = vec![];
    while let Some(item) = reader.read().unwrap() {
        names.push(item.name.clone());
    }

    assert_eq!(
        names,
        vec!["item1", "item2", "item3", "item4", "item5"],
        "should read all items across multiple pages with keyset via SelectBuilder"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn should_write_row_with_null_optional_column() -> Result<(), sqlx::Error> {
    let pool = SqlitePool::connect("sqlite::memory:").await?;
    sqlx::query("CREATE TABLE items (id INTEGER NOT NULL, label TEXT)")
        .execute(&pool)
        .await?;

    #[derive(Debug, Clone, Serialize)]
    struct Item {
        id: i32,
        label: Option<String>,
    }

    let writer = RdbcItemWriterBuilder::<Item>::new()
        .sqlite(&pool)
        .table("items")
        .column("id", |i: &Item| i.id.into())
        .column("label", |i: &Item| i.label.clone().into())
        .build_sqlite();

    use spring_batch_rs::core::item::ItemWriter;
    writer.write(&[Item { id: 1, label: None }]).unwrap();
    writer
        .write(&[Item {
            id: 2,
            label: Some("hello".to_string()),
        }])
        .unwrap();

    let rows: Vec<(i32, Option<String>)> =
        sqlx::query_as("SELECT id, label FROM items ORDER BY id")
            .fetch_all(&pool)
            .await?;

    assert_eq!(rows.len(), 2, "should have inserted two rows");
    assert_eq!(rows[0], (1, None), "first row label should be NULL");
    assert_eq!(
        rows[1],
        (2, Some("hello".to_string())),
        "second row label should be 'hello'"
    );
    Ok(())
}
