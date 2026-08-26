# spring-batch-rs

> Stop writing batch boilerplate. Start processing data.

[![crate](https://img.shields.io/crates/v/spring-batch-rs.svg)](https://crates.io/crates/spring-batch-rs)
[![docs](https://docs.rs/spring-batch-rs/badge.svg)](https://docs.rs/spring-batch-rs)
[![build status](https://github.com/spring-batch-rs/sbrs-lib/actions/workflows/test.yml/badge.svg)](https://github.com/spring-batch-rs/sbrs-lib/actions/workflows/test.yml)
[![Discord chat](https://img.shields.io/discord/1097536141617528966.svg?logo=discord&style=flat-square)](https://discord.gg/9FNhawNsG6)
[![CodeCov](https://codecov.io/gh/spring-batch-rs/sbrs-lib/branch/main/graph/badge.svg)](https://codecov.io/gh/spring-batch-rs/sbrs-lib)
![license](https://shields.io/badge/license-MIT%2FApache--2.0-blue)

Processing a large CSV into a database? You end up writing readers, chunk logic, error
loops, retry handling — just to move data. **Spring Batch RS** handles the plumbing: you
define what to read, what to transform, where to write. Skip policies, execution metrics,
and fault tolerance come built-in.

## Quick Start

### 1. Add to `Cargo.toml`

```toml
[dependencies]
spring-batch-rs = { version = "0.3", features = ["csv", "json"] }
serde = { version = "1.0", features = ["derive"] }
```

### 2. Your first batch job (CSV → JSON)

> **Note:** `rdbc-*` and `orm` features require `tokio = { version = "1", features = ["full"] }`.
> See the [Getting Started guide](https://spring-batch-rs.boussekeyt.dev/getting-started/) for the async setup.

```rust
use spring_batch_rs::{
    core::{job::{Job, JobBuilder}, step::StepBuilder},
    item::{
        csv::csv_reader::CsvItemReaderBuilder,
        json::json_writer::JsonItemWriterBuilder,
    },
    BatchError,
};
use serde::{Deserialize, Serialize};
use std::env::temp_dir;

#[derive(Deserialize, Serialize, Clone)]
struct Order {
    id: u32,
    amount: f64,
    status: String,
}

fn main() -> Result<(), BatchError> {
    let csv = "id,amount,status\n1,99.5,pending\n2,14.0,complete\n3,bad,pending";

    // Read from CSV
    let reader = CsvItemReaderBuilder::<Order>::new()
        .has_headers(true)
        .from_reader(csv.as_bytes());

    // Write to JSON
    let output = temp_dir().join("orders.json");
    let writer = JsonItemWriterBuilder::<Order>::new()
        .from_path(&output);

    // Wire together: read 100 items at a time, tolerate up to 5 bad rows
    let step = StepBuilder::new("csv-to-json")
        .chunk::<Order, Order>(100)
        .reader(&reader)
        // No processor as input object is the same as output object
        .writer(&writer)
        .skip_limit(5)
        .build();

    JobBuilder::new().start(&step).build().run().map(|_| ())?;
    println!("Output: {}", output.display());
    Ok(())
}
```

## How It Works

A **Job** contains one or more **Steps**. Each Step reads items one by one from a source,
buffers them into a configurable chunk, then writes the whole chunk at once — balancing
throughput with memory usage.

```
Read item → Read item → ... → [chunk full] → Write chunk → repeat
```

## Why spring-batch-rs

- **Chunk-oriented processing** — reads one item at a time, writes in batches. Memory usage stays constant regardless of dataset size.
- **Fault tolerance built-in** — set a `skip_limit` to keep processing when bad rows appear. No manual try/catch loops.
- **Type-safe pipelines** — reader, processor, and writer types are verified at compile time. Mismatched types don't compile.
- **Modular by design** — enable only what you need via feature flags. No unused dependencies.

## Features

**Formats**

| Feature | Description |
| ------- | ----------- |
| `csv`   | CSV `ItemReader` and `ItemWriter` |
| `json`  | JSON `ItemReader` and `ItemWriter` |
| `xml`   | XML `ItemReader` and `ItemWriter` |

**Databases** *(require `tokio` — see [Getting Started](https://spring-batch-rs.boussekeyt.dev/getting-started/))*

| Feature         | Description |
| --------------- | ----------- |
| `rdbc-postgres` | PostgreSQL `ItemReader` and `ItemWriter` |
| `rdbc-mysql`    | MySQL / MariaDB `ItemReader` and `ItemWriter` |
| `rdbc-sqlite`   | SQLite `ItemReader` and `ItemWriter` |
| `mongodb`       | MongoDB `ItemReader` and `ItemWriter` (sync) |
| `orm`           | SeaORM `ItemReader` and `ItemWriter` |

**Utilities**

| Feature  | Description |
| -------- | ----------- |
| `zip`    | ZIP compression `Tasklet` |
| `ftp`    | FTP / FTPS `Tasklet` |
| `fake`   | Fake data `ItemReader` for generating test datasets |
| `logger` | Logger `ItemWriter` for debugging pipelines |
| `full`   | All of the above |

## Examples

| Use case | Run |
| -------- | --- |
| CSV → JSON | `cargo run --example csv_processing --features csv,json` |
| JSON processing | `cargo run --example json_processing --features json,csv,logger` |
| XML processing | `cargo run --example xml_processing --features xml,json,csv` |
| CSV → SQLite | `cargo run --example database_processing --features rdbc-sqlite,csv,json,logger` |
| MongoDB | `cargo run --example mongodb_processing --features mongodb,csv,json` |
| SeaORM | `cargo run --example orm_processing --features orm,csv,json` |
| Advanced ETL pipeline | `cargo run --example advanced_patterns --features csv,json,logger` |
| ZIP tasklet | `cargo run --example tasklet_zip --features zip` |
| FTP tasklet | `cargo run --example tasklet_ftp --features ftp` |

> Database examples require Docker. Browse the **[full examples gallery](https://spring-batch-rs.boussekeyt.dev/quick-examples/)** for tutorials and advanced patterns.

## Documentation

| Resource | Link |
| -------- | ---- |
| Getting Started | [spring-batch-rs.boussekeyt.dev/getting-started](https://spring-batch-rs.boussekeyt.dev/getting-started/) |
| Item Readers & Writers | [spring-batch-rs.boussekeyt.dev/item-readers-writers](https://spring-batch-rs.boussekeyt.dev/item-readers-writers/overview/) |
| API Reference | [docs.rs/spring-batch-rs](https://docs.rs/spring-batch-rs) |
| Architecture | [spring-batch-rs.boussekeyt.dev/architecture](https://spring-batch-rs.boussekeyt.dev/architecture/) |

## Community

- [Discord](https://discord.gg/9FNhawNsG6) — Chat with the community
- [GitHub Issues](https://github.com/sboussekeyt/spring-batch-rs/issues) — Bug reports and feature requests
- [GitHub Discussions](https://github.com/sboussekeyt/spring-batch-rs/discussions) — Questions and ideas

## License

Licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) at your option.
