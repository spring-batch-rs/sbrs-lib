use std::{
    cell::{Cell, RefCell},
    fs::File,
    io::{BufWriter, Write},
    marker::PhantomData,
    path::Path,
};

use crate::{
    BatchError,
    core::item::{ItemWriter, ItemWriterResult},
};

/// A writer that writes items to a JSON output.
///
/// The writer serializes items to JSON format and writes them as an array to the output.
/// It handles proper JSON formatting, including opening and closing brackets for the array
/// and separating items with commas.
///
/// # Examples
///
/// ```
/// use spring_batch_rs::item::json::json_writer::JsonItemWriterBuilder;
/// use spring_batch_rs::core::item::ItemWriter;
/// use serde::Serialize;
/// use std::io::Cursor;
///
/// // Define a data structure
/// #[derive(Serialize)]
/// struct Product {
///     id: u32,
///     name: String,
///     price: f64,
/// }
///
/// // Create some products to write
/// let products = vec![
///     Product { id: 1, name: "Widget".to_string(), price: 19.99 },
///     Product { id: 2, name: "Gadget".to_string(), price: 24.99 },
/// ];
///
/// // Create a writer to an in-memory buffer
/// let buffer = Cursor::new(Vec::new());
/// let writer = JsonItemWriterBuilder::<Product>::new()
///     .from_writer(buffer);
///
/// // Write the products to JSON
/// let writer_ref = &writer as &dyn ItemWriter<Product>;
/// writer_ref.open().unwrap();
/// writer_ref.write(&products).unwrap();
/// writer_ref.close().unwrap();
/// ```
pub struct JsonItemWriter<O, W: Write> {
    /// The buffered writer for the output stream
    stream: RefCell<BufWriter<W>>,
    /// Whether to use pretty formatting (indentation and newlines)
    use_pretty_formatter: bool,
    /// Custom indentation to use when pretty formatting
    indent: Box<[u8]>,
    /// Tracks whether we're writing the first element (to handle commas between items)
    is_first_element: Cell<bool>,
    _phantom: PhantomData<O>,
}

impl<O: serde::Serialize, W: Write> ItemWriter<O> for JsonItemWriter<O, W> {
    /// Writes a batch of items to the JSON output.
    ///
    /// This method serializes each item to JSON, adds commas between items,
    /// and writes the result to the output stream. It keeps track of whether
    /// it's writing the first element to properly format the array.
    ///
    /// # Parameters
    /// - `items`: A slice of items to be serialized and written
    ///
    /// # Returns
    /// - `Ok(())` if successful
    /// - `Err(BatchError)` if writing fails
    fn write(&self, items: &[O]) -> ItemWriterResult {
        let mut json_chunk = String::new();

        for item in items.iter() {
            if !self.is_first_element.get() {
                json_chunk.push(',');
            } else {
                self.is_first_element.set(false);
            }

            let result = if self.use_pretty_formatter {
                // Use custom indentation if pretty formatting is enabled
                let mut buf = Vec::new();
                let formatter = serde_json::ser::PrettyFormatter::with_indent(&self.indent);
                let mut ser = serde_json::Serializer::with_formatter(&mut buf, formatter);
                match item.serialize(&mut ser) {
                    Ok(_) => match String::from_utf8(buf) {
                        Ok(s) => Ok(s),
                        Err(e) => Err(BatchError::ItemWriter(e.to_string())),
                    },
                    Err(e) => Err(BatchError::ItemWriter(e.to_string())),
                }
            } else {
                serde_json::to_string(item).map_err(|e| BatchError::ItemWriter(e.to_string()))
            };

            match result {
                Ok(json_str) => json_chunk.push_str(&json_str),
                Err(e) => return Err(e),
            }

            if self.use_pretty_formatter {
                json_chunk.push('\n');
            }
        }

        let result = self.stream.borrow_mut().write_all(json_chunk.as_bytes());

        match result {
            Ok(_ser) => Ok(()),
            Err(error) => Err(BatchError::ItemWriter(error.to_string())),
        }
    }

    /// Flushes the output buffer to ensure all data is written to the underlying stream.
    ///
    /// # Returns
    /// - `Ok(())` if successful
    /// - `Err(BatchError)` if flushing fails
    fn flush(&self) -> ItemWriterResult {
        let result = self.stream.borrow_mut().flush();

        match result {
            Ok(()) => Ok(()),
            Err(error) => Err(BatchError::ItemWriter(error.to_string())),
        }
    }

    /// Opens the JSON writer and writes the opening array bracket.
    ///
    /// This method should be called before any calls to write().
    ///
    /// # Returns
    /// - `Ok(())` if successful
    /// - `Err(BatchError)` if writing fails
    fn open(&self) -> ItemWriterResult {
        let begin_array = if self.use_pretty_formatter {
            b"[\n".to_vec()
        } else {
            b"[".to_vec()
        };

        let result = self.stream.borrow_mut().write_all(&begin_array);

        match result {
            Ok(()) => Ok(()),
            Err(error) => Err(BatchError::ItemWriter(error.to_string())),
        }
    }

    /// Closes the JSON writer and writes the closing array bracket.
    ///
    /// This method should be called after all items have been written.
    /// It also flushes the buffer to ensure all data is written.
    ///
    /// # Returns
    /// - `Ok(())` if successful
    /// - `Err(BatchError)` if writing fails
    fn close(&self) -> ItemWriterResult {
        let end_array = if self.use_pretty_formatter {
            b"\n]\n".to_vec()
        } else {
            b"]\n".to_vec()
        };

        let result = self.stream.borrow_mut().write_all(&end_array);
        let _ = self.stream.borrow_mut().flush();

        match result {
            Ok(()) => Ok(()),
            Err(error) => Err(BatchError::ItemWriter(error.to_string())),
        }
    }
}

/// A builder for creating JSON item writers.
///
/// This builder provides a convenient way to configure and create a `JsonItemWriter`
/// with options like pretty formatting and custom indentation.
///
/// # Examples
///
/// ```
/// use spring_batch_rs::item::json::json_writer::JsonItemWriterBuilder;
/// use spring_batch_rs::core::item::ItemWriter;
/// use serde::Serialize;
/// use std::io::Cursor;
///
/// // Define a data structure
/// #[derive(Serialize)]
/// struct Person {
///     id: u32,
///     name: String,
///     email: String,
/// }
///
/// // Create a writer with pretty formatting
/// let buffer = Cursor::new(Vec::new());
/// let writer = JsonItemWriterBuilder::<Person>::new()
///     .pretty_formatter(true)
///     .from_writer(buffer);
///
/// // Use the writer to serialize a person
/// let person = Person {
///     id: 1,
///     name: "Alice".to_string(),
///     email: "alice@example.com".to_string(),
/// };
///
/// let writer_ref = &writer as &dyn ItemWriter<Person>;
/// writer_ref.open().unwrap();
/// writer_ref.write(&[person]).unwrap();
/// writer_ref.close().unwrap();
/// ```
pub struct JsonItemWriterBuilder<O> {
    /// Indentation to use when pretty-printing (default is two spaces)
    indent: Box<[u8]>,
    /// Whether to use pretty formatting with indentation and newlines
    pretty_formatter: bool,
    /// Phantom data for the output type
    _pd: PhantomData<O>,
}

impl<O> Default for JsonItemWriterBuilder<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O> JsonItemWriterBuilder<O> {
    /// Creates a new JSON item writer builder with default settings.
    ///
    /// By default, the writer uses compact formatting (not pretty-printed)
    /// and a standard indentation of two spaces.
    ///
    /// # Examples
    ///
    /// ```
    /// use spring_batch_rs::item::json::json_writer::JsonItemWriterBuilder;
    ///
    /// let builder = JsonItemWriterBuilder::<String>::new();
    /// ```
    pub fn new() -> Self {
        Self {
            indent: Box::from(b"  ".to_vec()),
            pretty_formatter: false,
            _pd: PhantomData,
        }
    }

    /// Sets the indentation to use when pretty-printing JSON.
    ///
    /// This setting only has an effect if pretty formatting is enabled.
    ///
    /// # Examples
    ///
    /// ```
    /// use spring_batch_rs::item::json::json_writer::JsonItemWriterBuilder;
    ///
    /// // Use 4 spaces for indentation
    /// let builder = JsonItemWriterBuilder::<String>::new()
    ///     .indent(b"    ")
    ///     .pretty_formatter(true);
    /// ```
    pub fn indent(mut self, indent: &[u8]) -> Self {
        self.indent = Box::from(indent);
        self
    }

    /// Enables or disables pretty formatting of the JSON output.
    ///
    /// When enabled, the JSON output will include newlines and indentation
    /// to make it more human-readable. This is useful for debugging or
    /// when the output will be read by humans.
    ///
    /// # Examples
    ///
    /// ```
    /// use spring_batch_rs::item::json::json_writer::JsonItemWriterBuilder;
    ///
    /// // Enable pretty printing
    /// let pretty_builder = JsonItemWriterBuilder::<String>::new()
    ///     .pretty_formatter(true);
    ///
    /// // Disable pretty printing for compact output
    /// let compact_builder = JsonItemWriterBuilder::<String>::new()
    ///     .pretty_formatter(false);
    /// ```
    pub fn pretty_formatter(mut self, yes: bool) -> Self {
        self.pretty_formatter = yes;
        self
    }

    /// Creates a JSON item writer that writes to a file at the specified path.
    ///
    /// This method creates a new file (or truncates an existing one) and
    /// configures a JsonItemWriter to write to it.
    ///
    /// # Parameters
    /// - `path`: The path where the output file will be created
    ///
    /// # Returns
    /// A configured JsonItemWriter instance
    ///
    /// # Panics
    /// Panics if the file cannot be created
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use spring_batch_rs::item::json::json_writer::JsonItemWriterBuilder;
    /// use spring_batch_rs::core::item::ItemWriter;
    /// use serde::Serialize;
    /// use std::path::Path;
    ///
    /// #[derive(Serialize)]
    /// struct Record {
    ///     id: u32,
    ///     data: String,
    /// }
    ///
    /// // Create a writer to a file
    /// let writer = JsonItemWriterBuilder::<Record>::new()
    ///     .pretty_formatter(true)
    ///     .from_path("output.json");
    ///
    /// // Generate some data
    /// let records = vec![
    ///     Record { id: 1, data: "First record".to_string() },
    ///     Record { id: 2, data: "Second record".to_string() },
    /// ];
    ///
    /// // Write the data to the file
    /// let writer_ref = &writer as &dyn ItemWriter<Record>;
    /// writer_ref.open().unwrap();
    /// writer_ref.write(&records).unwrap();
    /// writer_ref.close().unwrap();
    /// ```
    pub fn from_path<W: AsRef<Path>>(self, path: W) -> JsonItemWriter<O, File> {
        let file = File::create(path).expect("Unable to open file");

        let buf_writer = BufWriter::new(file);

        JsonItemWriter {
            stream: RefCell::new(buf_writer),
            use_pretty_formatter: self.pretty_formatter,
            indent: self.indent.clone(),
            is_first_element: Cell::new(true),
            _phantom: PhantomData,
        }
    }

    /// Creates a JSON item writer that writes to any destination implementing the `Write` trait.
    ///
    /// This allows writing to in-memory buffers, network connections, or other custom destinations.
    ///
    /// # Parameters
    /// - `wtr`: The writer instance to use for output
    ///
    /// # Returns
    /// A configured JsonItemWriter instance
    ///
    /// # Examples
    ///
    /// ```
    /// use spring_batch_rs::item::json::json_writer::JsonItemWriterBuilder;
    /// use spring_batch_rs::core::item::ItemWriter;
    /// use serde::Serialize;
    /// use std::io::Cursor;
    ///
    /// #[derive(Serialize)]
    /// struct Event {
    ///     timestamp: u64,
    ///     message: String,
    /// }
    ///
    /// // Create a writer to an in-memory buffer
    /// let buffer = Cursor::new(Vec::new());
    /// let writer = JsonItemWriterBuilder::<Event>::new()
    ///     .from_writer(buffer);
    ///
    /// // Generate some data
    /// let events = vec![
    ///     Event { timestamp: 1620000000, message: "Server started".to_string() },
    ///     Event { timestamp: 1620000060, message: "Connected to database".to_string() },
    /// ];
    ///
    /// // Write the data
    /// let writer_ref = &writer as &dyn ItemWriter<Event>;
    /// writer_ref.open().unwrap();
    /// writer_ref.write(&events).unwrap();
    /// writer_ref.close().unwrap();
    /// ```
    pub fn from_writer<W: Write>(self, wtr: W) -> JsonItemWriter<O, W> {
        let buf_writer = BufWriter::new(wtr);

        JsonItemWriter {
            stream: RefCell::new(buf_writer),
            use_pretty_formatter: self.pretty_formatter,
            indent: self.indent,
            is_first_element: Cell::new(true),
            _phantom: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::item::ItemWriter;
    use serde::Serialize;
    use std::fs;
    use tempfile::tempdir;

    #[derive(Serialize, Debug, PartialEq)]
    struct TestItem {
        id: u32,
        name: String,
        value: f64,
    }

    #[test]
    fn json_writer_builder_should_create_with_defaults() {
        let builder = JsonItemWriterBuilder::<TestItem>::new();
        assert!(!builder.pretty_formatter);
        assert_eq!(builder.indent, b"  ".to_vec().into_boxed_slice());
    }

    #[test]
    fn json_writer_builder_should_set_pretty_formatter() {
        let builder = JsonItemWriterBuilder::<TestItem>::new().pretty_formatter(true);
        assert!(builder.pretty_formatter);
    }

    #[test]
    fn json_writer_builder_should_set_custom_indent() {
        let custom_indent = b"    ";
        let builder = JsonItemWriterBuilder::<TestItem>::new().indent(custom_indent);
        assert_eq!(builder.indent, custom_indent.to_vec().into_boxed_slice());
    }

    #[test]
    fn json_writer_builder_should_implement_default() {
        let builder1 = JsonItemWriterBuilder::<TestItem>::new();
        let builder2 = JsonItemWriterBuilder::<TestItem>::default();

        assert_eq!(builder1.pretty_formatter, builder2.pretty_formatter);
        // Both should have the same default indent
        assert_eq!(builder1.indent, builder2.indent);
    }

    #[test]
    fn json_writer_builder_should_support_generic_type() {
        let _builder = JsonItemWriterBuilder::<TestItem>::new();
        let _builder_default = JsonItemWriterBuilder::<TestItem>::default();
    }

    #[test]
    fn json_writer_from_path_should_create_file_writer() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test_output.json");

        let writer: JsonItemWriter<TestItem, File> =
            JsonItemWriterBuilder::new().from_path(&file_path);

        let item = TestItem {
            id: 1,
            name: "test".to_string(),
            value: 42.5,
        };

        writer.open().unwrap();
        writer.write(&[item]).unwrap();
        writer.close().unwrap();

        // Verify file was created and contains expected content
        let content = fs::read_to_string(&file_path).unwrap();
        assert!(content.contains(r#"{"id":1,"name":"test","value":42.5}"#));
    }

    #[test]
    fn json_writer_should_handle_custom_indent() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("indent_test.json");

        let writer = JsonItemWriterBuilder::new()
            .pretty_formatter(true)
            .indent(b"\t")
            .from_path(&file_path);

        let item = TestItem {
            id: 1,
            name: "test".to_string(),
            value: 42.5,
        };

        writer.open().unwrap();
        writer.write(&[item]).unwrap();
        writer.close().unwrap();

        let content = fs::read_to_string(&file_path).unwrap();
        // Check that the content uses tab indentation in pretty format
        // The JSON should contain tab characters for indentation
        assert!(content.contains('\t'));
    }

    #[test]
    fn json_writer_should_handle_pretty_formatting() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("pretty_test.json");

        let writer = JsonItemWriterBuilder::new()
            .pretty_formatter(true)
            .from_path(&file_path);

        let item = TestItem {
            id: 1,
            name: "test".to_string(),
            value: 42.5,
        };

        writer.open().unwrap();
        writer.write(&[item]).unwrap();
        writer.close().unwrap();

        let content = fs::read_to_string(&file_path).unwrap();
        assert!(content.contains("[\n"));
        assert!(content.contains("\n]\n"));
        assert!(content.contains("  \"id\": 1"));
    }

    #[test]
    fn json_writer_should_handle_empty_items() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("empty_test.json");

        let writer = JsonItemWriterBuilder::new().from_path(&file_path);
        let empty_items: Vec<TestItem> = vec![];

        writer.open().unwrap();
        writer.write(&empty_items).unwrap();
        writer.close().unwrap();

        let content = fs::read_to_string(&file_path).unwrap();
        assert_eq!(content, "[]\n");
    }

    #[test]
    fn json_writer_should_handle_multiple_writes() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("multi_test.json");

        let writer = JsonItemWriterBuilder::new().from_path(&file_path);

        let item1 = TestItem {
            id: 1,
            name: "first".to_string(),
            value: 10.0,
        };
        let item2 = TestItem {
            id: 2,
            name: "second".to_string(),
            value: 20.0,
        };

        writer.open().unwrap();
        writer.write(&[item1]).unwrap();
        writer.write(&[item2]).unwrap();
        writer.close().unwrap();

        let content = fs::read_to_string(&file_path).unwrap();
        assert!(content.contains(r#"{"id":1,"name":"first","value":10.0}"#));
        assert!(content.contains(r#"{"id":2,"name":"second","value":20.0}"#));
        assert!(content.contains(','));
    }

    #[test]
    fn json_writer_should_write_to_in_memory_buffer() {
        use std::io::Cursor;

        let buf = Cursor::new(Vec::new());
        let writer = JsonItemWriterBuilder::<TestItem>::new().from_writer(buf);

        let item = TestItem {
            id: 7,
            name: "cursor".to_string(),
            value: 0.5,
        };
        writer.open().unwrap();
        writer.write(&[item]).unwrap();
        writer.close().unwrap();
        // Reaching here without panic confirms from_writer + write + close work
    }

    #[test]
    fn json_writer_should_flush_without_error() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("flush_test.json");

        let writer = JsonItemWriterBuilder::<TestItem>::new().from_path(&file_path);
        writer.open().unwrap();
        writer.flush().unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn json_writer_compact_open_writes_bracket_without_newline() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("compact_open.json");

        let writer = JsonItemWriterBuilder::<TestItem>::new()
            .pretty_formatter(false)
            .from_path(&file_path);
        writer.open().unwrap();
        writer.close().unwrap();

        let content = fs::read_to_string(&file_path).unwrap();
        assert_eq!(
            content, "[]\n",
            "compact format should produce []\\n, got: {content:?}"
        );
    }

    // A writer that always fails on any write or flush
    struct FailWriter;
    impl std::io::Write for FailWriter {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "write failed",
            ))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "flush failed",
            ))
        }
    }

    fn fail_json_writer<O: Serialize>() -> JsonItemWriter<O, FailWriter> {
        JsonItemWriter {
            stream: RefCell::new(BufWriter::with_capacity(0, FailWriter)),
            use_pretty_formatter: false,
            indent: Box::from(b"  ".as_slice()),
            is_first_element: Cell::new(true),
            _phantom: PhantomData,
        }
    }

    #[test]
    fn should_return_error_when_open_fails_on_io() {
        let writer = fail_json_writer::<String>();
        let result = ((&writer) as &dyn ItemWriter<String>).open();
        assert!(result.is_err(), "open should fail when writer fails");
    }

    #[test]
    fn should_return_error_when_close_fails_on_io() {
        let writer = fail_json_writer::<String>();
        let result = ((&writer) as &dyn ItemWriter<String>).close();
        assert!(result.is_err(), "close should fail when writer fails");
    }

    #[test]
    fn should_return_error_when_flush_fails_on_io() {
        let writer = fail_json_writer::<String>();
        let result = ((&writer) as &dyn ItemWriter<String>).flush();
        assert!(result.is_err(), "flush should fail when writer fails");
    }

    #[test]
    fn should_return_error_when_write_fails_on_io() {
        let writer = fail_json_writer::<String>();
        // write() serializes first (into a String), then writes to stream
        // With capacity-0 BufWriter, write_all on the stream fails
        let result = ((&writer) as &dyn ItemWriter<String>).write(&["hello".to_string()]);
        assert!(
            result.is_err(),
            "write should fail when underlying IO fails"
        );
    }

    #[test]
    fn should_return_error_when_serialization_fails_with_pretty_formatter() {
        use crate::BatchError;

        struct NonSerializable;
        impl Serialize for NonSerializable {
            fn serialize<S: serde::Serializer>(&self, _s: S) -> Result<S::Ok, S::Error> {
                Err(serde::ser::Error::custom(
                    "intentional serialization failure",
                ))
            }
        }

        let buf = std::io::Cursor::new(Vec::new());
        let writer = JsonItemWriterBuilder::<NonSerializable>::new()
            .pretty_formatter(true)
            .from_writer(buf);
        let result = ((&writer) as &dyn ItemWriter<NonSerializable>).write(&[NonSerializable]);
        match result.err().unwrap() {
            BatchError::ItemWriter(msg) => assert!(
                msg.contains("intentional"),
                "error should contain serialization message, got: {msg}"
            ),
            e => panic!("expected ItemWriter error, got {e:?}"),
        }
    }
}
