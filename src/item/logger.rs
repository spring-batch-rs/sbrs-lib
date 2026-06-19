use std::fmt::Debug;
use std::marker::PhantomData;

use log::info;

use crate::core::item::{ItemWriter, ItemWriterResult};

/// A simple item writer that logs the items using the `log` crate.
///
/// This writer is created using the [`LoggerWriterBuilder`].
///
/// # Examples
///
/// ```
/// use spring_batch_rs::item::logger::LoggerWriterBuilder;
/// use spring_batch_rs::core::item::ItemWriter;
///
/// let writer = LoggerWriterBuilder::<i32>::new().build();
/// let items = vec![1, 2, 3];
/// let result = writer.write(&items);
/// assert!(result.is_ok());
/// ```
pub struct LoggerWriter<T> {
    _pd: PhantomData<T>,
}

impl<T: Debug> ItemWriter<T> for LoggerWriter<T> {
    /// Writes the items to the log.
    ///
    /// # Arguments
    ///
    /// * `items` - The items to be written.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the items were successfully written to the log.
    ///
    /// # Examples
    ///
    /// ```
    /// use spring_batch_rs::item::logger::LoggerWriterBuilder;
    /// use spring_batch_rs::core::item::{ItemWriter, ItemWriterResult};
    ///
    /// let writer = LoggerWriterBuilder::<i32>::new().build();
    /// let items = vec![1, 2, 3];
    /// let result: ItemWriterResult = writer.write(&items);
    /// assert!(result.is_ok());
    /// ```
    fn write(&self, items: &[T]) -> ItemWriterResult {
        items.iter().for_each(|item| info!("Record:{:?}", item));
        Ok(())
    }
}

/// A builder for creating logger writers.
///
/// This builder provides a consistent API for creating [`LoggerWriter`] instances,
/// following the same pattern as other writers in the framework.
///
/// # Examples
///
/// ```
/// use spring_batch_rs::item::logger::LoggerWriterBuilder;
/// use spring_batch_rs::core::item::ItemWriter;
///
/// let writer = LoggerWriterBuilder::<i32>::new().build();
/// writer.write(&[1, 2, 3]).unwrap();
/// ```
///
/// Using with custom types:
///
/// ```
/// use spring_batch_rs::item::logger::LoggerWriterBuilder;
/// use spring_batch_rs::core::item::ItemWriter;
///
/// #[derive(Debug)]
/// struct Record {
///     id: u32,
///     value: String,
/// }
///
/// let writer = LoggerWriterBuilder::<Record>::new().build();
/// let items = vec![
///     Record { id: 1, value: "test".to_string() },
/// ];
/// writer.write(&items).unwrap();
/// ```
#[derive(Default)]
pub struct LoggerWriterBuilder<T> {
    _pd: PhantomData<T>,
}

impl<T> LoggerWriterBuilder<T> {
    /// Creates a new logger writer builder.
    ///
    /// # Examples
    ///
    /// ```
    /// use spring_batch_rs::item::logger::LoggerWriterBuilder;
    ///
    /// let builder = LoggerWriterBuilder::<String>::new();
    /// ```
    pub fn new() -> Self {
        Self { _pd: PhantomData }
    }

    /// Builds a [`LoggerWriter`] instance.
    ///
    /// # Returns
    ///
    /// A configured [`LoggerWriter`] instance.
    ///
    /// # Examples
    ///
    /// ```
    /// use spring_batch_rs::item::logger::LoggerWriterBuilder;
    /// use spring_batch_rs::core::item::ItemWriter;
    ///
    /// let writer = LoggerWriterBuilder::<i32>::new().build();
    /// writer.write(&[1, 2, 3]).unwrap();
    /// ```
    pub fn build(self) -> LoggerWriter<T> {
        LoggerWriter { _pd: PhantomData }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write() {
        let writer = LoggerWriterBuilder::<i32>::new().build();
        let items = vec![1, 2, 3];
        let result = writer.write(&items);
        assert!(result.is_ok());
    }

    #[test]
    fn test_builder_new() {
        let builder = LoggerWriterBuilder::<String>::new();
        let _writer = builder.build();
    }

    #[test]
    fn test_builder_default() {
        let builder = LoggerWriterBuilder::<String>::default();
        let _writer = builder.build();
    }

    #[test]
    fn test_write_with_custom_type() {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct Record {
            id: u32,
        }

        let writer = LoggerWriterBuilder::<Record>::new().build();
        let items = vec![Record { id: 1 }];
        let result = writer.write(&items);
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_empty_items() {
        let writer = LoggerWriterBuilder::<i32>::new().build();
        let items: Vec<i32> = vec![];
        let result = writer.write(&items);
        assert!(result.is_ok());
    }
}
