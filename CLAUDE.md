# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Spring Batch RS is a Rust implementation of the Spring Batch framework for building enterprise-grade batch processing applications. It provides chunk-oriented processing, extensible readers/writers, and support for multiple data formats and databases.

**Version**: 0.5.0
**Language**: Rust 2024 Edition
**Documentation**: https://spring-batch-rs.boussekeyt.dev/

## Essential Commands

### Development Workflow
```bash
# Complete development cycle (format, lint, test)
make dev

# Run all quality checks (format check, clippy, audit)
make check

# Run all tests with all features
make test

# Run tests with specific feature combinations
make test-features
```

### Building
```bash
# Build in release mode with all features
make build

# Build in debug mode
make build-dev

# Build all examples
make examples
```

### Code Quality
```bash
# Format code with rustfmt
make format

# Run clippy lints (zero warnings policy)
make lint

# Run security audit
make audit

# Generate test coverage report (requires cargo-tarpaulin)
make coverage
```

### Documentation
```bash
# Generate and open rustdoc
make doc

# Start website dev server at http://localhost:4321
make website-serve

# Build production website
make website-build
```

### Running Examples
```bash
# Example: CSV to JSON conversion
cargo run --example generate_json_file_from_csv_string_with_fault_tolerance --features csv,json

# Example: Database operations
cargo run --example log_records_from_postgres_database --features rdbc-postgres,logger

# See all available examples
make examples-run
```

### Running Individual Tests
```bash
# Run specific test by name
cargo test test_name --all-features

# Run tests for a specific module
cargo test csv_integration --all-features

# Run tests with specific features only
cargo test --features csv,json

# Run a single integration test file
cargo test --test csv_integration --all-features
```

## Architecture Overview

### Core Concepts

The framework follows a layered architecture with these key abstractions:

- **Job**: Container for batch process composed of one or more steps
- **Step**: Independent phase of a job (chunk-oriented or tasklet-based)
- **ItemReader**: Reads items one at a time from a data source
- **ItemProcessor**: Transforms items (business logic)
- **ItemWriter**: Writes chunks of items to a destination
- **Tasklet**: Single-task operations outside chunk-oriented pattern

### Processing Model

**Chunk-Oriented Processing** (primary pattern):
```
Read → Process → Buffer → Write (in chunks)
```

The chunk processor reads items one-by-one, processes each, buffers them, and writes the entire chunk. This balances performance (fewer I/O operations) with memory usage (controlled by chunk size).

**Tasklet-Based Processing** (for single tasks):
Used for operations like ZIP compression, FTP transfers, or any operation that doesn't fit the read-process-write pattern.

### Code Structure

```
src/
├── core/           # Core abstractions (Job, Step, Item traits)
├── item/           # Format/database-specific implementations
│   ├── csv/        # CSV reader/writer
│   ├── json/       # JSON reader/writer
│   ├── xml/        # XML reader/writer
│   ├── rdbc/       # Database connectivity (PostgreSQL, MySQL, SQLite)
│   ├── mongodb/    # MongoDB support (synchronous only)
│   ├── orm/        # SeaORM integration
│   ├── fake/       # Fake data generation
│   └── logger/     # Debug logging writer
├── tasklet/        # Single-task operations (ZIP, FTP)
└── error.rs        # Custom BatchError enum

tests/              # Integration tests with testcontainers
examples/           # 24+ practical examples
website/            # Astro + Starlight documentation site
```

### Feature Flags

The project uses feature flags for modular compilation. Always specify required features:

**Data Formats**: `csv`, `json`, `xml`
**Databases**: `rdbc-postgres`, `rdbc-mysql`, `rdbc-sqlite`, `mongodb`, `orm`
**Utilities**: `zip`, `ftp`, `fake`, `logger`
**Meta**: `full` (all features), `tests-cfg` (for testing)

Example: `cargo build --features csv,json,rdbc-postgres`

## Specialized Rule Files

Detailed coding rules are maintained in separate files — follow them precisely:

- @.claude/rules/01-rustdoc.md — Rustdoc comment standards (structure, sections, doc-tests)
- @.claude/rules/02-unit-tests.md — Inline unit test rules (`#[cfg(test)]` modules, naming, coverage)
- @.claude/rules/03-examples.md — Example file conventions (naming, structure, Cargo.toml)
- @.claude/rules/04-documentation.md — Documentation sync rules (rustdoc + website + README)

## Development Guidelines

### Error Handling

Always use the `BatchError` enum for batch-related errors:
```rust
use crate::BatchError;

pub fn operation(&self) -> Result<T, BatchError> {
    let result = fallible_op()
        .map_err(|e| BatchError::ItemReader(format!("Context: {}", e)))?;
    Ok(result)
}
```

Error variants:
- `BatchError::ItemReader` - Reading errors
- `BatchError::ItemWriter` - Writing errors
- `BatchError::ItemProcessor` - Processing errors
- `BatchError::Step` - Step-level errors
- `BatchError::Job` - Job-level errors

### Builder Pattern

Complex objects use the builder pattern:
```rust
let step = StepBuilder::new("step-name")
    .chunk(100)
    .reader(&reader)
    .processor(&processor)
    .writer(&writer)
    .skip_limit(10)
    .build();

let job = JobBuilder::new()
    .start(&step)
    .build();
```

### Logging

**Never use `println!`** - always use the `log` macros:
```rust
use log::{debug, error, info, warn};

info!("Starting step: {}", step_name);
debug!("Processed {} items", count);
error!("Failed to write chunk: {}", error);
```

### Testing Requirements

- **Target: 96%+ code coverage** for public APIs
- Use `mockall` for mocking dependencies
- Use `testcontainers` for database integration tests
- All doc tests must compile and run successfully
- Test both success and error scenarios

Test pattern:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use mockall::mock;

    #[test]
    fn should_handle_success_case() {
        // Arrange
        let mock = MockComponent::new();

        // Act
        let result = operation(&mock);

        // Assert
        assert!(result.is_ok());
    }
}
```

### Code Quality Standards

**Pre-commit requirements** (enforced by CI):
```bash
cargo fmt --all -- --check          # Code formatting
cargo clippy --all-features -- -D warnings  # Zero clippy warnings
cargo test --all-features           # All tests pass
cargo doc --no-deps --all-features  # Documentation builds
cargo audit                         # No security issues
```

Run `make dev` to execute format, lint, and test in one command.

### Website Sync — MANDATORY

**Any time a new feature, reader, writer, tasklet, or example is added or modified, the website MUST be updated in the same change.** This is non-negotiable.

Required updates per change type:

| Change | Files to update |
|---|---|
| New tasklet | `website/src/content/docs/tasklets/index.md` + `examples/tasklets.mdx` + `reference/features.mdx` |
| New reader/writer | `website/src/content/docs/item-readers-writers/overview.mdx` + `reference/features.mdx` |
| New example | `website/src/content/docs/examples/<category>.mdx` |
| New feature flag | `reference/features.mdx` + README.md features table |

See `@.claude/rules/04-documentation.md` for the full sync checklist.

---

### Documentation Requirements

All public APIs must have rustdoc comments with:
- Brief description
- `# Examples` section with runnable code
- `# Errors` section documenting error conditions
- `# Panics` section if applicable

Example:
```rust
/// Reads items from a CSV file.
///
/// # Examples
///
/// ```rust
/// use spring_batch_rs::item::csv::CsvItemReaderBuilder;
///
/// let reader = CsvItemReaderBuilder::<Product>::new()
///     .has_headers(true)
///     .from_path("products.csv");
/// ```
///
/// # Errors
///
/// Returns [`BatchError::ItemReader`] if the file cannot be read or parsed.
pub fn read(&self) -> Result<Option<T>, BatchError> {
    // ...
}
```

## Important Implementation Details

### Memory Management

Chunk size controls memory usage. The framework reads items one-by-one, buffers them up to the chunk size, then writes the entire chunk. Adjust chunk size based on item size and available memory.

### Async vs Sync

- **Most operations**: Use tokio async runtime
- **MongoDB**: Synchronous only (uses `mongodb/sync` feature)
- When mixing, use `tokio::task::spawn_blocking` for MongoDB operations

### Database Testing

Integration tests use testcontainers to spin up real databases:
```rust
use testcontainers_modules::postgres::Postgres;

let container = Postgres::default().start().await?;
let connection_string = format!(
    "postgresql://postgres:postgres@127.0.0.1:{}/postgres",
    container.get_host_port_ipv4(5432).await?
);
```

### Extension Points

To add custom functionality, implement the core traits:

- `ItemReader<T>` - Custom data sources
- `ItemProcessor<I, O>` - Custom transformations
- `ItemWriter<T>` - Custom destinations
- `Tasklet` - Custom single-task operations

All implementations should follow the builder pattern and use `BatchError` for errors.

## CI/CD Pipeline

GitHub Actions workflows:
- **test.yml**: Run tests on all feature combinations
- **clippy.yml**: Lint with clippy (zero warnings)
- **fmt.yml**: Check code formatting
- **audit.yml**: Security audit with cargo-audit
- **docs.yml**: Generate and deploy documentation to GitHub Pages
- **build.yml**: Verify build succeeds

All PRs must pass all checks before merging.

## Troubleshooting

### Tests failing with database errors
Ensure Docker is running for testcontainers integration tests.

### Feature compilation errors
Check that you've enabled the correct features. Use `--all-features` or specify required features explicitly.

### Documentation build fails
Ensure all doc tests compile. Run `cargo test --doc --all-features` to verify.

### Coverage generation fails
Install cargo-tarpaulin: `cargo install cargo-tarpaulin`
