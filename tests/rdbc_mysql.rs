mod helpers;

use std::{io::Read, path::Path};

use anyhow::Error;
use helpers::{
    common::{DEFAULT_CHUNK_SIZE, EXPECTED_PERSON_COUNT, EXPECTED_PERSON_CSV, SAMPLE_CARS_CSV},
    mysql_helpers::{CREATE_CARS_TABLE_SQL, Car, SELECT_ALL_CARS_SQL},
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
        rdbc::{MySqlRdbcItemReader, RdbcItemReaderBuilder, RdbcItemWriterBuilder},
    },
};
use sqlx::{FromRow, MySqlPool, migrate::Migrator};
use tempfile::NamedTempFile;
use testcontainers_modules::{mysql, testcontainers::runners::AsyncRunner};

#[derive(Serialize, Deserialize, Clone, FromRow)]
struct Person {
    id: Option<i32>,
    first_name: String,
    last_name: String,
}

#[derive(Debug, Clone, PartialEq, FromRow)]
struct TestUser {
    id: i32,
    name: String,
    email: String,
}

async fn setup_reader_test_db() -> Result<
    (
        MySqlPool,
        testcontainers_modules::testcontainers::ContainerAsync<mysql::Mysql>,
    ),
    Box<dyn std::error::Error>,
> {
    let container = mysql::Mysql::default().start().await?;
    let host_ip = container.get_host().await?;
    let host_port = container.get_host_port_ipv4(3306).await?;
    let pool = MySqlPool::connect(&format!("mysql://{}:{}/test", host_ip, host_port)).await?;

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS test_users (
            id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            email VARCHAR(255) NOT NULL
        )",
    )
    .execute(&pool)
    .await?;

    for i in 1..=10 {
        sqlx::query("INSERT INTO test_users (name, email) VALUES (?, ?)")
            .bind(format!("User{}", i))
            .bind(format!("user{}@test.com", i))
            .execute(&pool)
            .await?;
    }

    Ok((pool, container))
}

#[tokio::test(flavor = "multi_thread")]
async fn read_items_from_database() -> Result<(), Error> {
    // Prepare container
    let container = mysql::Mysql::default().start().await?;
    let host_ip = container.get_host().await?;
    let host_port = container.get_host_port_ipv4(3306).await?;

    // Prepare database
    let connection_uri = format!("mysql://{}:{}/test", host_ip, host_port);
    let pool = MySqlPool::connect(&connection_uri).await?;
    let migrator = Migrator::new(Path::new("tests/migrations/mysql")).await?;
    migrator.run(&pool).await?;

    // Prepare reader using unified builder
    let query = "SELECT * from person";
    let reader = RdbcItemReaderBuilder::new()
        .mysql(pool.clone())
        .query(query)
        .with_page_size(5)
        .build_mysql();

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
async fn write_items_to_database() -> Result<(), Error> {
    // Prepare container
    let container = mysql::Mysql::default().start().await?;
    let host_ip = container.get_host().await?;
    let host_port = container.get_host_port_ipv4(3306).await?;

    // Prepare reader
    let reader = CsvItemReaderBuilder::<Car>::new()
        .has_headers(true)
        .from_reader(SAMPLE_CARS_CSV.as_bytes());

    // Prepare writer
    let connection_uri = format!("mysql://{}:{}/test", host_ip, host_port);
    let pool = MySqlPool::connect(&connection_uri).await?;

    // Create table
    sqlx::query(CREATE_CARS_TABLE_SQL).execute(&pool).await?;

    let writer = RdbcItemWriterBuilder::<Car>::new()
        .mysql(&pool)
        .table("cars")
        .column("year", |c: &Car| c.year.into())
        .column("make", |c: &Car| c.make.as_str().into())
        .column("model", |c: &Car| c.model.as_str().into())
        .column("description", |c: &Car| c.description.as_str().into())
        .build_mysql();

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
async fn mysql_reader_should_read_all_items_with_keyset_pagination()
-> Result<(), Box<dyn std::error::Error>> {
    let (pool, _container) = setup_reader_test_db().await?;

    let reader: MySqlRdbcItemReader<TestUser> = MySqlRdbcItemReader::new(
        pool,
        "SELECT id, name, email FROM test_users".to_string(),
        Some(3),
        Some("id".to_string()),
        Some(Box::new(|u: &TestUser| u.id.to_string())),
    );

    let mut items = Vec::new();
    while let Some(item) = reader.read()? {
        items.push(item);
    }

    assert_eq!(
        items.len(),
        10,
        "keyset pagination should return all 10 rows"
    );
    for (i, item) in items.iter().enumerate() {
        assert_eq!(item.id, (i + 1) as i32);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mysql_reader_should_cross_page_boundary_with_keyset()
-> Result<(), Box<dyn std::error::Error>> {
    let (pool, _container) = setup_reader_test_db().await?;

    // page_size=4 with 10 rows means 3 pages — exercises cursor update across boundaries
    let reader: MySqlRdbcItemReader<TestUser> = MySqlRdbcItemReader::new(
        pool,
        "SELECT id, name, email FROM test_users".to_string(),
        Some(4),
        Some("id".to_string()),
        Some(Box::new(|u: &TestUser| u.id.to_string())),
    );

    let mut ids = Vec::new();
    while let Some(item) = reader.read()? {
        ids.push(item.id);
    }

    assert_eq!(ids.len(), 10, "all 10 rows should be returned");
    assert_eq!(ids, (1..=10).collect::<Vec<_>>(), "IDs should be in order");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mysql_reader_should_return_none_for_empty_table_with_keyset()
-> Result<(), Box<dyn std::error::Error>> {
    let (pool, _container) = setup_reader_test_db().await?;

    let reader: MySqlRdbcItemReader<TestUser> = MySqlRdbcItemReader::new(
        pool,
        "SELECT id, name, email FROM test_users WHERE id > 9999".to_string(),
        Some(5),
        Some("id".to_string()),
        Some(Box::new(|u: &TestUser| u.id.to_string())),
    );

    let result = reader.read()?;
    assert!(result.is_none(), "empty keyset result should yield None");

    Ok(())
}
