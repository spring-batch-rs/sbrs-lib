pub mod common;

use std::{
    env::temp_dir,
    fs::{self, File, read_to_string},
    path::Path,
};

use ::serde::{Deserialize, Serialize, ser::Error};
use rand::distr::{Alphanumeric, SampleString};
use serde::Serializer;

use spring_batch_rs::{
    core::{
        item::{ItemProcessor, ItemProcessorResult},
        job::{Job, JobBuilder},
        step::{StepBuilder, StepStatus},
    },
    item::csv::csv_reader::CsvItemReaderBuilder,
    item::csv::csv_writer::CsvItemWriterBuilder,
    item::json::json_reader::JsonItemReaderBuilder,
    item::json::json_writer::JsonItemWriterBuilder,
};
use time::{Date, Month, format_description};

fn date_serializer<S>(date: &Date, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let format = format_description::parse_borrowed::<2>("[year]-[month]-[day]")
        .map_err(|e| Error::custom(e.to_string()))?;
    let s = date
        .format(&format)
        .map_err(|e| Error::custom(e.to_string()))?;
    serializer.serialize_str(&s)
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Person {
    first_name: String,
    last_name: String,
    title: String,
    email: String,
    #[serde(serialize_with = "date_serializer")]
    birth_date: Date,
}

#[derive(Default)]
struct UpperCaseProcessor;

impl ItemProcessor<Person, Person> for UpperCaseProcessor {
    fn process(&self, item: Person) -> ItemProcessorResult<Person> {
        let person = Person {
            first_name: item.first_name.to_uppercase(),
            last_name: item.last_name.to_uppercase(),
            title: item.title.to_uppercase(),
            email: item.email.to_uppercase(),
            birth_date: Date::from_calendar_date(2019, Month::January, 1).unwrap(),
        };

        Ok(Some(person))
    }
}

#[derive(Default)]
struct CarProcessor;

impl ItemProcessor<Car, Car> for CarProcessor {
    fn process(&self, item: Car) -> ItemProcessorResult<Car> {
        Ok(Some(item))
    }
}

#[test]
fn transform_from_json_file_to_csv_file_without_error() {
    let path = Path::new("examples/data/persons.json");

    let file = File::open(path).expect("Unable to open file");

    let reader = JsonItemReaderBuilder::new().from_reader(file);

    let processor = UpperCaseProcessor;

    let writer = CsvItemWriterBuilder::new()
        .has_headers(true)
        .from_path(temp_dir().join("persons.csv"));

    let step = StepBuilder::new("test")
        .chunk(3)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run();
    assert!(result.is_ok());

    let file_content = read_to_string(temp_dir().join("persons.csv"))
        .expect("Should have been able to read the file");

    assert_eq!(
        file_content,
        "first_name,last_name,title,email,birth_date
OCÉANE,DUPOND,MR.,LEOPOLD_ENIM@ORANGE.FR,2019-01-01
AMANDINE,ÉVRAT,MRS.,AMANDINE_IURE@OUTLOOK.FR,2019-01-01
UGO,NIELS,SIR.,XAVIER_VOLUPTATEM@SFR.FR,2019-01-01
LÉO,ZOLA,DR.,UGO_PRAESENTIUM@ORANGE.FR,2019-01-01
"
    );
}

#[derive(Deserialize, Serialize, Debug)]
struct Car {
    year: u16,
    make: String,
    model: String,
    description: String,
}

#[test]
fn convert_csv_file_to_json_file_without_error() {
    let path = Path::new("examples/data/cars_with_headers.csv");

    let file = File::open(path).expect("Unable to open file");

    let reader = CsvItemReaderBuilder::<Car>::new()
        .has_headers(true)
        .from_reader(file);

    let processor = CarProcessor;

    let writer = JsonItemWriterBuilder::new().from_path(temp_dir().join("cars.json"));

    let step = StepBuilder::new("test")
        .chunk::<Car, Car>(3)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run();
    assert!(result.is_ok());

    let step_execution = job.get_step_execution("test").unwrap();

    assert!(step_execution.status == StepStatus::Success);
    assert!(step_execution.read_count == 7);
    assert!(step_execution.write_count == 7);
    assert!(step_execution.process_count == 7);
    assert!(step_execution.read_error_count == 0);
    assert!(step_execution.write_error_count == 0);

    let file_content = fs::read_to_string(temp_dir().join("cars.json"))
        .expect("Should have been able to read the file");

    assert_eq!(
        file_content,
        r#"[{"year":1948,"make":"Porsche","model":"356","description":"Luxury sports car 1"},{"year":1949,"make":"Porsche","model":"357","description":"Luxury sports car 2"},{"year":1950,"make":"Porsche","model":"358","description":"Luxury sports car 3"},{"year":1951,"make":"Porsche","model":"359","description":"Luxury sports car 4"},{"year":1952,"make":"Porsche","model":"360","description":"Luxury sports car 5"},{"year":1967,"make":"Ford","model":"Mustang fastback 1967","description":"American car"},{"year":1967,"make":"Ford","model":"Mustang fastback 1967","description":"American car"}]
"#
    );
}

#[test]
fn transform_csv_stream_to_writer_with_one_error_should_succeded() {
    let csv = "year,make,model,description
    1948,Porsche,356,Luxury sports car
    2011d,Peugeot,206+,City car
    2012,Citroën,C4 Picasso,SUV
    2021,Mazda,CX-30,SUV Compact
    1967,Ford,Mustang fastback 1967,American car";

    let reader = CsvItemReaderBuilder::<Car>::new()
        .has_headers(true)
        .from_reader(csv.as_bytes());

    let file_name = Alphanumeric.sample_string(&mut rand::rng(), 16);

    let file_writer =
        File::create(temp_dir().join(file_name.clone())).expect("Unable to open file");

    let writer = JsonItemWriterBuilder::new().from_writer(file_writer);

    let processor = CarProcessor;

    let step = StepBuilder::new("test")
        .chunk::<Car, Car>(3)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .skip_limit(1)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run();
    assert!(result.is_ok());

    let step_execution = job.get_step_execution("test").unwrap();

    assert!(step_execution.status == StepStatus::Success);
    assert!(step_execution.read_count == 4);
    assert!(step_execution.write_count == 4);
    assert!(step_execution.read_error_count == 1);
    assert!(step_execution.write_error_count == 0);

    let file_content = fs::read_to_string(temp_dir().join(file_name))
        .expect("Should have been able to read the file");

    assert_eq!(
        file_content,
        r#"[{"year":1948,"make":"Porsche","model":"356","description":"Luxury sports car"},{"year":2012,"make":"Citroën","model":"C4 Picasso","description":"SUV"},{"year":2021,"make":"Mazda","model":"CX-30","description":"SUV Compact"},{"year":1967,"make":"Ford","model":"Mustang fastback 1967","description":"American car"}]
"#
    );
}

#[test]
fn transform_csv_stream_to_writer_with_3_errors_should_failed() {
    let csv = "year,make,model,description
    1948,Porsche,356,Luxury sports car
    2011d,Peugeot,206+,City car
    2012,Citroën,C4 Picasso,SUV
    1995,Peugeot,205,City car
    2021d,Mazda,CX-30,SUV Compact
    1967d,Ford,Mustang fastback 1967,American car";

    let reader = CsvItemReaderBuilder::<Car>::new()
        .has_headers(true)
        .from_reader(csv.as_bytes());

    let file_name = Alphanumeric.sample_string(&mut rand::rng(), 16);

    let file_writer =
        File::create(temp_dir().join(file_name.clone())).expect("Unable to open file");

    let writer = JsonItemWriterBuilder::new().from_writer(file_writer);

    let processor = CarProcessor;

    let step = StepBuilder::new("test")
        .chunk(3)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .skip_limit(2)
        .build();

    let job = JobBuilder::new().start(&step).build();
    let result = job.run();
    assert!(result.is_err());
    let step_execution = job.get_step_execution("test").unwrap();

    assert!(step_execution.status == StepStatus::ReadError);
    assert!(step_execution.read_count == 3);
    assert!(step_execution.write_count == 3);
    assert!(step_execution.read_error_count == 3);
    assert!(step_execution.write_error_count == 0);

    let file_content = fs::read_to_string(temp_dir().join(file_name))
        .expect("Should have been able to read the file");

    assert_eq!(
        file_content,
        r#"[{"year":1948,"make":"Porsche","model":"356","description":"Luxury sports car"},{"year":2012,"make":"Citroën","model":"C4 Picasso","description":"SUV"},{"year":1995,"make":"Peugeot","model":"205","description":"City car"}]
"#
    );
}
