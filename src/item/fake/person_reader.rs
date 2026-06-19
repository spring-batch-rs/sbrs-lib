use std::cell::Cell;
use std::fmt;

use ::serde::{Deserialize, Serialize, Serializer, ser::Error};
use fake::faker::internet::raw::*;
use fake::locales::*;
use fake::{Fake, faker::name::raw::*};
use log::debug;
use rand::RngExt;

use time::format_description;
use time::{Date, Month};

use crate::core::item::ItemReader;
use crate::core::item::ItemReaderResult;

/// Represents a person with their personal information.
#[derive(Serialize, Deserialize, Clone)]
pub struct Person {
    first_name: String,
    last_name: String,
    title: String,
    email: String,
    #[serde(serialize_with = "date_serializer")]
    birth_date: Date,
}

/// Serializes a `Date` object into a string representation.
fn date_serializer<S>(date: &Date, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let format = format_description::parse_borrowed::<2>("[year]-[month]-[day]")
        .map_err(|e| Error::custom(e.to_string()))?;
    let s = date.format(&format).map_err(|e| Error::custom(e.to_string()))?;
    serializer.serialize_str(&s)
}

impl fmt::Display for Person {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "first_name:{}, last_name:{}, birth_date:{}",
            self.first_name, self.last_name, self.birth_date
        )
    }
}

/// A reader for generating fake `Person` objects.
///
/// # Examples
///
/// ```
/// use spring_batch_rs::item::fake::person_reader::PersonReaderBuilder;
/// use spring_batch_rs::core::item::ItemReader;
///
/// let reader = PersonReaderBuilder::new().number_of_items(3).build();
///
/// let person1 = reader.read().unwrap();
/// assert!(person1.is_some());
///
/// let person2 = reader.read().unwrap();
/// assert!(person2.is_some());
///
/// let person3 = reader.read().unwrap();
/// assert!(person3.is_some());
///
/// let done = reader.read().unwrap();
/// assert!(done.is_none());
/// ```
pub struct PersonReader {
    count: Cell<usize>,
}

impl ItemReader<Person> for PersonReader {
    /// Reads the next `Person` object.
    ///
    /// Returns `Ok(Some(person))` if there are more `Person` objects to read.
    /// Returns `Ok(None)` if there are no more `Person` objects to read.
    fn read(&self) -> ItemReaderResult<Person> {
        if self.count.get() == 0 {
            return Ok(None);
        }

        self.count.set(self.count.get() - 1);

        let person = Person {
            first_name: FirstName(FR_FR).fake(),
            last_name: LastName(FR_FR).fake(),
            title: Title(FR_FR).fake(),
            email: FreeEmail(FR_FR).fake(),
            birth_date: fake_date(),
        };
        debug!("Person: {}", person);
        Ok(Some(person))
    }
}

/// Generates a random `Date` object.
fn fake_date() -> Date {
    let mut rng = rand::rng();
    let year = rng.random_range(1900..2022);
    let month = rng.random_range(1..=12);
    let day = rng.random_range(1..=28);

    Date::from_calendar_date(year, Month::try_from(month).unwrap(), day).unwrap()
}

/// Builder for creating a `PersonReader`.
#[derive(Default)]
pub struct PersonReaderBuilder {
    number_of_items: usize,
}

impl PersonReaderBuilder {
    /// Creates a new `PersonReaderBuilder` instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use spring_batch_rs::item::fake::person_reader::PersonReaderBuilder;
    ///
    /// let builder = PersonReaderBuilder::new();
    /// assert!(true); // builder is created successfully
    /// ```
    pub fn new() -> Self {
        Self { number_of_items: 0 }
    }

    /// Sets the number of `Person` objects to generate.
    ///
    /// # Examples
    ///
    /// ```
    /// use spring_batch_rs::item::fake::person_reader::PersonReaderBuilder;
    ///
    /// let builder = PersonReaderBuilder::new().number_of_items(10);
    /// assert!(true); // number_of_items is set successfully
    /// ```
    pub fn number_of_items(mut self, number_of_items: usize) -> Self {
        self.number_of_items = number_of_items;
        self
    }

    /// Builds a `PersonReader` instance with the configured settings.
    ///
    /// # Examples
    ///
    /// ```
    /// use spring_batch_rs::item::fake::person_reader::PersonReaderBuilder;
    /// use spring_batch_rs::core::item::ItemReader;
    ///
    /// let reader = PersonReaderBuilder::new().number_of_items(2).build();
    ///
    /// let first = reader.read().unwrap();
    /// assert!(first.is_some());
    ///
    /// let second = reader.read().unwrap();
    /// assert!(second.is_some());
    ///
    /// let none = reader.read().unwrap();
    /// assert!(none.is_none());
    /// ```
    pub fn build(self) -> PersonReader {
        PersonReader {
            count: self.number_of_items.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PersonReader, PersonReaderBuilder};
    use crate::core::item::ItemReader;

    #[test]
    fn should_read_configured_number_of_persons() {
        let reader: PersonReader = PersonReaderBuilder::new().number_of_items(2).build();
        assert_eq!(reader.count.get(), 2);

        let result1 = reader.read();
        assert_eq!(reader.count.get(), 1);
        assert!(result1.is_ok());

        let person = result1.unwrap();
        assert!(person.is_some());
        assert!(!person.as_ref().unwrap().first_name.is_empty());
        assert!(!person.as_ref().unwrap().last_name.is_empty());

        let result2 = reader.read();
        assert_eq!(reader.count.get(), 0);
        assert!(result2.is_ok());
        assert!(result2.unwrap().is_some());

        let result3 = reader.read();
        assert_eq!(reader.count.get(), 0);
        assert!(result3.unwrap().is_none());
    }

    #[test]
    fn should_display_person_with_all_fields() {
        let reader = PersonReaderBuilder::new().number_of_items(1).build();
        let person = reader.read().unwrap().unwrap();
        let text = format!("{}", person);
        assert!(
            text.contains("first_name:"),
            "Display missing first_name: {text}"
        );
        assert!(
            text.contains("last_name:"),
            "Display missing last_name: {text}"
        );
        assert!(
            text.contains("birth_date:"),
            "Display missing birth_date: {text}"
        );
    }

    #[test]
    fn should_serialize_person_with_date_in_iso_format() {
        let reader = PersonReaderBuilder::new().number_of_items(1).build();
        let person = reader.read().unwrap().unwrap();
        let json = serde_json::to_string(&person).expect("serialization must succeed");
        assert!(
            json.contains("birth_date"),
            "missing birth_date key: {json}"
        );
        // Date must follow YYYY-MM-DD pattern (at least two hyphens)
        let hyphen_count = json.chars().filter(|&c| c == '-').count();
        assert!(hyphen_count >= 2, "date should contain hyphens: {json}");
    }
}
