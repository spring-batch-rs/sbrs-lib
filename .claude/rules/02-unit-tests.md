# Unit Test Rules — spring-batch-rs

## Requirement

Every source file in `src/` that contains public logic MUST have an inline `#[cfg(test)]` module. Integration tests in `tests/` are complementary, not a substitute.

## Files that currently have inline tests (reference)

- `src/core/item.rs` ✓
- `src/core/step.rs` ✓
- `src/core/job.rs` ✓
- `src/error.rs` ✓
- `src/item/csv/csv_reader.rs` ✓
- `src/item/json/json_reader.rs` ✓
- `src/item/json/json_writer.rs` ✓
- `src/item/xml/xml_reader.rs` ✓
- `src/item/xml/xml_writer.rs` ✓
- `src/item/logger.rs` ✓
- `src/item/rdbc/writer_common.rs` ✓
- `src/item/rdbc/reader_common.rs` ✓
- `src/tasklet/zip.rs` ✓
- `src/tasklet/ftp.rs` ✓

## Files that MUST have inline tests added

- `src/item/rdbc/postgres_reader.rs` — no tests (use mocks)
- `src/item/rdbc/mysql_reader.rs` — no tests (use mocks)
- `src/item/rdbc/sqlite_reader.rs` — no tests (use mocks)
- `src/item/rdbc/postgres_writer.rs` — builder unit tests only
- `src/item/rdbc/mysql_writer.rs` — builder unit tests only
- `src/item/rdbc/sqlite_writer.rs` — builder unit tests only
- `src/item/orm/orm_writer.rs` — builder unit tests only
- `src/item/fake/person_reader.rs` — unit tests only

## Module Structure

```rust
#[cfg(test)]
mod tests {
    use super::*;
    // import only what this module needs

    // --- helpers / shared data ---

    fn sample_records() -> Vec<MyStruct> { ... }

    // --- happy path ---

    #[test]
    fn should_write_records_without_error() { ... }

    // --- edge cases ---

    #[test]
    fn should_handle_empty_input() { ... }

    // --- error paths ---

    #[test]
    fn should_return_error_on_malformed_data() { ... }
}
```

## Naming Convention

Test names use `should_<behaviour>_<condition>` (snake_case):

```
should_read_typed_records_from_csv_string
should_return_none_at_end_of_file
should_return_error_on_malformed_row
should_write_headers_when_configured
should_flush_buffer_after_write
```

## Coverage Target: 96 %

Every public method needs at least:
1. One happy-path test
2. One edge-case test (empty input, boundary values)
3. One error-path test (when the method can return `Err`)

## Mocking Database Dependencies

RDBC readers/writers depend on `sqlx` connection pools. Use `mockall` to avoid requiring Docker in unit tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mockall::predicate::*;

    // Test only builder configuration — no actual DB connection
    #[test]
    fn should_build_reader_with_page_size() {
        // Verify builder state, not execution
        // Full execution is tested in tests/rdbc_postgres.rs
    }
}
```

## CSV Writer Test Pattern

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::item::ItemWriter;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Row {
        name: String,
        value: u32,
    }

    fn sample_rows() -> Vec<Row> {
        vec![
            Row { name: "alpha".into(), value: 1 },
            Row { name: "beta".into(), value: 2 },
        ]
    }

    #[test]
    fn should_write_with_headers() {
        let mut buf = Vec::new();
        {
            let writer = CsvItemWriterBuilder::<Row>::new()
                .has_headers(true)
                .from_writer(&mut buf);
            writer.write(&sample_rows()).unwrap();
            ItemWriter::<Row>::flush(&writer).unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("name,value"), "missing header row");
        assert!(out.contains("alpha,1"));
        assert!(out.contains("beta,2"));
    }

    #[test]
    fn should_write_without_headers() {
        let mut buf = Vec::new();
        {
            let writer = CsvItemWriterBuilder::<Row>::new()
                .has_headers(false)
                .from_writer(&mut buf);
            writer.write(&sample_rows()).unwrap();
            ItemWriter::<Row>::flush(&writer).unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        assert!(!out.contains("name"), "header row should be absent");
    }

    #[test]
    fn should_write_empty_chunk_without_error() {
        let mut buf = Vec::new();
        {
            let writer = CsvItemWriterBuilder::<Row>::new()
                .from_writer(&mut buf);
            writer.write(&[]).unwrap();
            ItemWriter::<Row>::flush(&writer).unwrap();
        }
        let out = String::from_utf8(buf).unwrap();
        assert!(out.trim().is_empty() || out.contains("name"), "unexpected content");
    }
}
```

## Assert Style

- Use `assert_eq!` over `assert!(a == b)` — gives better failure messages.
- Always include a message string in assertions that aren't self-evident:
  ```rust
  assert!(output.contains("Alice"), "expected name 'Alice' in output: {output}");
  ```
- Do NOT use `unwrap()` in tests without a comment explaining why it cannot fail.

## Forbidden Patterns

```rust
// WRONG: test name says nothing
#[test]
fn test1() { ... }

// WRONG: no assertion
#[test]
fn should_build_reader() {
    let _ = CsvItemReaderBuilder::<Record>::new().from_reader("".as_bytes());
    // no assert — test always passes regardless of correctness
}

// WRONG: println! instead of assert
#[test]
fn should_read() {
    let item = reader.read().unwrap();
    println!("{item:?}"); // not a test, just debug output
}
```
