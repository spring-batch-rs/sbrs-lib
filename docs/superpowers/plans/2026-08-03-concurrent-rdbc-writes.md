# Concurrent RDBC Writes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the PostgreSQL and MySQL item writers issue several chunk INSERTs concurrently, opt-in via `.with_concurrency(N)`, so the dominant write phase stops being a queue of one.

**Architecture:** The concurrency mechanism lives in one new module, `inflight_writes.rs`, which owns a bounded `JoinSet` and knows nothing about SQL. Each writer keeps building its own query and hands the resulting future to that module. The engine is changed so a failing `close()` fails the step — without this, the final in-flight batch could fail silently.

**Tech Stack:** Rust 2024, `tokio::task::JoinSet`, `sqlx` 0.9, `mockall` (dev), existing helpers in `src/item/rdbc/writer_common.rs`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-08-03-concurrent-rdbc-writes-design.md`.
- Branch: `feat-concurrent-rdbc-writes` (already created; spec committed as `979aa66`).
- **Opt-in only.** `with_concurrency(1)` or no call at all must take the *existing code path*, not an equivalent one. Verify by branching on `concurrency <= 1` before any new logic runs.
- **Scope is PostgreSQL and MySQL.** SQLite warns and stays sequential. SeaORM and MongoDB are untouched.
- **`flush()` must never drain.** The engine calls it after every chunk (`step.rs:955`); draining there collapses the design back to sequential. Only `close()` drains.
- Timing/counters: the engine attributes `write_error_count += processed_items.len()` (`step.rs:963`) to the *current* chunk even when the error came from an earlier one. Document, do not try to fix.
- Version target **0.5.0** — the `close()` change is a behavioural break.
- Rust 2024, zero-warning clippy (`-D warnings`), `cargo fmt --check` clean.
- Never `println!`; use `log` macros.
- Test naming `should_<behaviour>_<condition>`; every test and doc-test asserts something.
- **`block_in_place` panics on a current-thread runtime.** Every test that exercises the concurrent path must be `#[tokio::test(flavor = "multi_thread")]`.

---

### Task 1: Make a failing close() fail the step

**Files:**
- Modify: `src/core/step.rs:729` (the `close()` call in `ChunkOrientedStep::execute`)
- Test: `src/core/step.rs` inline `mod tests`

**Interfaces:**
- Consumes: nothing.
- Produces: `ChunkOrientedStep::execute` returns `Err(BatchError::Step(name))` and sets `StepStatus::WriteError` when `writer.close()` fails. Task 4 and Task 5 rely on this to surface the final batch's errors.

**Why first:** every later task depends on this being true. Concurrent writes learn the fate of their last batch only at `close()`; if that error stays a warning, the feature loses data silently.

- [ ] **Step 1: Write the failing test**

Add to the inline `mod tests` in `src/core/step.rs`:

```rust
#[test]
fn should_fail_the_step_when_close_fails() {
    let mut reader = MockTestItemReader::default();
    let mut counter = 0u16;
    reader.expect_read().returning(move || {
        counter += 1;
        if counter > 2 { Ok(None) } else { Ok(sample_car()) }
    });

    let processor = PassThroughProcessor::<Car>::new();

    let mut writer = MockTestItemWriter::default();
    writer.expect_open().returning(|| Ok(()));
    writer.expect_write().returning(|_| Ok(()));
    writer.expect_flush().returning(|| Ok(()));
    writer
        .expect_close()
        .returning(|| Err(BatchError::ItemWriter("close failed".to_string())));

    let step = StepBuilder::new("close-fails")
        .chunk(10)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let mut step_execution = StepExecution::new("close-fails");
    let result = step.execute(&mut step_execution);

    assert!(
        result.is_err(),
        "a failing close() must fail the step, otherwise unwritten data is reported as success"
    );
    assert_eq!(step_execution.status, StepStatus::WriteError);
}

#[test]
fn should_still_succeed_when_close_succeeds() {
    let mut reader = MockTestItemReader::default();
    let mut counter = 0u16;
    reader.expect_read().returning(move || {
        counter += 1;
        if counter > 2 { Ok(None) } else { Ok(sample_car()) }
    });

    let processor = PassThroughProcessor::<Car>::new();

    let mut writer = MockTestItemWriter::default();
    writer.expect_open().returning(|| Ok(()));
    writer.expect_write().returning(|_| Ok(()));
    writer.expect_flush().returning(|| Ok(()));
    writer.expect_close().returning(|| Ok(()));

    let step = StepBuilder::new("close-ok")
        .chunk(10)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let mut step_execution = StepExecution::new("close-ok");
    assert!(step.execute(&mut step_execution).is_ok());
    assert_eq!(step_execution.status, StepStatus::Success);
}
```

- [ ] **Step 2: Run tests to verify the first fails**

Run: `cargo test --all-features --lib core::step::tests::should_fail_the_step_when_close_fails`
Expected: FAIL — the step currently returns `Ok` because `manage_error` only warns.

- [ ] **Step 3: Write minimal implementation**

In `ChunkOrientedStep::execute`, replace the `close()` line (`step.rs:729`):

```rust
        Self::manage_error(self.writer.close());
```

with:

```rust
        // A failing close() means buffered or in-flight writes never landed.
        // Treat it as fatal: reporting Success here would hide data loss.
        let close_result = self.writer.close();
        if let Err(ref error) = close_result {
            warn!("Error closing writer: {}", error);
            step_execution.status = StepStatus::WriteError;
        }
```

Leave `Self::manage_error(self.writer.open());` (`step.rs:694`) exactly as it is — `open()` keeps its non-fatal handling, and widening the change serves nothing.

The existing status check at the end of `execute` already turns a non-`Success` status into `Err(BatchError::Step(...))`, so no further change is needed there. Verify that by reading the tail of `execute` before assuming it.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --all-features --lib core::step`
Expected: PASS. If any pre-existing test now fails because it relied on a failing `close()` being ignored, do NOT weaken the new behaviour — report it to the controller, since that test encodes the defect being fixed.

- [ ] **Step 5: Commit**

```bash
git add src/core/step.rs
git commit -m "fix!: fail the step when ItemWriter::close() fails

close() errors were swallowed by manage_error, so a step could report
Success after its final flush failed. Behavioural break: a job that
previously passed with a close error now fails."
```

---

### Task 2: InflightWrites — the bounded concurrency mechanism

**Files:**
- Create: `src/item/rdbc/inflight_writes.rs`
- Modify: `src/item/rdbc/mod.rs` (add `mod inflight_writes;` next to the other private modules at lines 2-20)
- Test: inline `#[cfg(test)] mod tests` in the new file

**Interfaces:**
- Consumes: `create_write_error(table: &str, db_name: &str, error: impl Display) -> BatchError` from `writer_common.rs:68`.
- Produces, used by Tasks 4 and 5:
  - `pub(crate) struct InflightWrites`
  - `pub(crate) fn new(limit: usize, table: String, db_name: &'static str) -> InflightWrites`
  - `pub(crate) fn spawn<F>(&mut self, fut: F) -> Result<(), BatchError> where F: Future<Output = Result<(), sqlx::Error>> + Send + 'static`
  - `pub(crate) fn harvest(&mut self) -> Result<(), BatchError>`
  - `pub(crate) fn drain(&mut self) -> Result<(), BatchError>`

**Why its own module:** this mechanism is identical for PostgreSQL and MySQL. Putting it in one place keeps the two writers free of `JoinSet` bookkeeping, and — the real win — it is testable with plain futures, needing no database and no Docker.

- [ ] **Step 1: Write the failing tests**

Create `src/item/rdbc/inflight_writes.rs` with only this test module at first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn ok_after(ms: u64) -> impl std::future::Future<Output = Result<(), sqlx::Error>> + Send + 'static {
        async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            Ok(())
        }
    }

    fn fail_after(ms: u64) -> impl std::future::Future<Output = Result<(), sqlx::Error>> + Send + 'static {
        async move {
            tokio::time::sleep(Duration::from_millis(ms)).await;
            Err(sqlx::Error::PoolClosed)
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_overlap_writes_up_to_the_limit() {
        let mut inflight = InflightWrites::new(4, "t".to_string(), "TestDb");

        let start = Instant::now();
        for _ in 0..4 {
            inflight.spawn(ok_after(100)).unwrap();
        }
        inflight.drain().unwrap();
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_millis(300),
            "4 x 100ms writes with limit 4 should overlap, took {:?}",
            elapsed
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_block_when_the_limit_is_reached() {
        let mut inflight = InflightWrites::new(2, "t".to_string(), "TestDb");

        let start = Instant::now();
        for _ in 0..4 {
            inflight.spawn(ok_after(100)).unwrap();
        }
        inflight.drain().unwrap();
        let elapsed = start.elapsed();

        assert!(
            elapsed >= Duration::from_millis(200),
            "4 x 100ms writes with limit 2 must serialise into 2 waves, took {:?}",
            elapsed
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_surface_error_on_drain() {
        let mut inflight = InflightWrites::new(4, "orders".to_string(), "TestDb");

        inflight.spawn(ok_after(10)).unwrap();
        inflight.spawn(fail_after(10)).unwrap();

        let result = inflight.drain();

        assert!(result.is_err(), "drain must surface a failed write");
        let message = result.unwrap_err().to_string();
        assert!(
            message.contains("TestDb"),
            "error should name the database, got: {message}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_report_no_error_when_nothing_was_spawned() {
        let mut inflight = InflightWrites::new(4, "t".to_string(), "TestDb");

        assert!(inflight.harvest().is_ok());
        assert!(inflight.drain().is_ok());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn should_not_block_on_harvest() {
        let mut inflight = InflightWrites::new(4, "t".to_string(), "TestDb");
        inflight.spawn(ok_after(500)).unwrap();

        let start = Instant::now();
        let result = inflight.harvest();
        let elapsed = start.elapsed();

        assert!(result.is_ok());
        assert!(
            elapsed < Duration::from_millis(50),
            "harvest must not wait for in-flight tasks, took {:?}",
            elapsed
        );

        inflight.drain().unwrap();
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Add `mod inflight_writes;` to `src/item/rdbc/mod.rs`, then run:
`cargo test --all-features --lib item::rdbc::inflight_writes`
Expected: FAIL to compile — `InflightWrites` does not exist.

- [ ] **Step 3: Write minimal implementation**

Prepend to `src/item/rdbc/inflight_writes.rs`:

```rust
//! Bounded concurrency for chunk writes.
//!
//! Holds at most `limit` in-flight write tasks. Errors are surfaced when
//! harvested — on a later call, or at [`InflightWrites::drain`] — never at the
//! moment the failing write was dispatched.

use std::future::Future;

use tokio::task::JoinSet;

use crate::BatchError;
use crate::item::rdbc::writer_common::create_write_error;

/// A bounded set of in-flight database writes.
///
/// # Implementation Note
///
/// `spawn` blocks when the limit is reached; that block *is* the backpressure,
/// so no separate counter is maintained. `harvest` never blocks, because the
/// step engine calls `ItemWriter::flush` after every chunk and blocking there
/// would serialise everything again.
pub(crate) struct InflightWrites {
    limit: usize,
    set: JoinSet<Result<(), sqlx::Error>>,
    table: String,
    db_name: &'static str,
}

impl InflightWrites {
    pub(crate) fn new(limit: usize, table: String, db_name: &'static str) -> Self {
        Self {
            limit: limit.max(1),
            set: JoinSet::new(),
            table,
            db_name,
        }
    }

    /// Dispatches a write, first waiting for a slot if the limit is reached.
    ///
    /// # Errors
    ///
    /// Returns [`BatchError::ItemWriter`] if a *previously* dispatched write
    /// failed and was harvested while making room.
    pub(crate) fn spawn<F>(&mut self, fut: F) -> Result<(), BatchError>
    where
        F: Future<Output = Result<(), sqlx::Error>> + Send + 'static,
    {
        let mut first_error = self.harvest();

        while self.set.len() >= self.limit {
            let joined = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(self.set.join_next())
            });
            match joined {
                Some(outcome) => {
                    if let Err(e) = self.interpret(outcome) {
                        if first_error.is_ok() {
                            first_error = Err(e);
                        }
                    }
                }
                None => break,
            }
        }

        self.set.spawn(fut);
        first_error
    }

    /// Collects results of already-finished writes without waiting.
    pub(crate) fn harvest(&mut self) -> Result<(), BatchError> {
        let mut first_error = Ok(());
        while let Some(outcome) = self.set.try_join_next() {
            if let Err(e) = self.interpret(outcome) {
                if first_error.is_ok() {
                    first_error = Err(e);
                }
            }
        }
        first_error
    }

    /// Waits for every in-flight write and returns the first error found.
    pub(crate) fn drain(&mut self) -> Result<(), BatchError> {
        let mut first_error = Ok(());
        loop {
            let joined = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(self.set.join_next())
            });
            match joined {
                Some(outcome) => {
                    if let Err(e) = self.interpret(outcome) {
                        if first_error.is_ok() {
                            first_error = Err(e);
                        }
                    }
                }
                None => break,
            }
        }
        first_error
    }

    fn interpret(
        &self,
        outcome: Result<Result<(), sqlx::Error>, tokio::task::JoinError>,
    ) -> Result<(), BatchError> {
        match outcome {
            Ok(Ok(())) => Ok(()),
            Ok(Err(e)) => Err(create_write_error(&self.table, self.db_name, e)),
            Err(join_error) => Err(create_write_error(&self.table, self.db_name, join_error)),
        }
    }
}
```

Note `limit.max(1)`: a zero limit would make `spawn` loop forever waiting for a slot it can never get.

`writer_common` is a private module, so `create_write_error` is reachable as
`crate::item::rdbc::writer_common::create_write_error`. If the compiler rejects that path, check
how the sibling writers import it and match them rather than making the module public.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --all-features --lib item::rdbc::inflight_writes`
Expected: PASS, 5 tests.

If `should_block_when_the_limit_is_reached` is flaky on a loaded machine, report it rather than loosening the threshold — the assertion is the only proof that bounding works.

- [ ] **Step 5: Commit**

```bash
git add src/item/rdbc/inflight_writes.rs src/item/rdbc/mod.rs
git commit -m "feat: add InflightWrites, a bounded set of in-flight writes"
```

---

### Task 3: with_concurrency on the builder

**Files:**
- Modify: `src/item/rdbc/unified_writer_builder.rs` (struct at :98, `new` at :109, and the three `build_*` at :252, :291, :330)
- Modify: `src/item/rdbc/postgres_writer.rs:43-58` (struct + `new`), `src/item/rdbc/mysql_writer.rs` (same shape)
- Test: inline `mod tests` in `unified_writer_builder.rs`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `RdbcItemWriterBuilder::with_concurrency(self, concurrency: usize) -> Self`, and a `pub(crate) concurrency: usize` field on `PostgresItemWriter<O>` and `MySqlItemWriter<O>`, defaulting to `1`. Tasks 4 and 5 read that field.

**This task adds no behaviour.** The field is carried and defaulted; nothing consumes it yet. Keeping it separate means a reviewer can check the API surface without reading concurrency logic.

- [ ] **Step 1: Write the failing tests**

Add to the inline `mod tests` in `src/item/rdbc/unified_writer_builder.rs`:

```rust
#[test]
fn should_default_concurrency_to_one() {
    let writer = RdbcItemWriterBuilder::<TestItem>::new()
        .table("items")
        .column("id", |i: &TestItem| ColumnValue::Int(i.id as i64))
        .build_postgres();

    assert_eq!(
        writer.concurrency, 1,
        "default must be sequential so nobody opts in by accident"
    );
}

#[test]
fn should_carry_configured_concurrency_to_the_writer() {
    let writer = RdbcItemWriterBuilder::<TestItem>::new()
        .table("items")
        .column("id", |i: &TestItem| ColumnValue::Int(i.id as i64))
        .with_concurrency(4)
        .build_postgres();

    assert_eq!(writer.concurrency, 4);
}

#[test]
fn should_force_sqlite_to_stay_sequential() {
    let writer = RdbcItemWriterBuilder::<TestItem>::new()
        .table("items")
        .column("id", |i: &TestItem| ColumnValue::Int(i.id as i64))
        .with_concurrency(8)
        .build_sqlite();

    // SqliteItemWriter has no concurrency field at all — this test asserts the
    // builder compiles and yields a working writer, i.e. the setting is
    // accepted and ignored rather than rejected.
    assert!(writer.table.is_some());
}
```

`TestItem` already exists in that test module (see the struct near `unified_writer_builder.rs:364`); reuse it rather than defining another.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --all-features --lib item::rdbc::unified_writer_builder`
Expected: FAIL to compile — no `with_concurrency`, no `concurrency` field.

- [ ] **Step 3: Write minimal implementation**

In `unified_writer_builder.rs`, add the field to the struct (after `column_bindings`, :104):

```rust
    concurrency: usize,
```

Initialise it in `new()` (:109):

```rust
            concurrency: 1,
```

Add the setter next to `table` (:197):

```rust
    /// Sets how many chunk writes may be in flight at once.
    ///
    /// Defaults to `1`, which keeps the sequential behaviour: each `write` call
    /// completes its INSERT before returning. Values above `1` dispatch writes
    /// concurrently over the connection pool, which changes two observable things:
    ///
    /// - Chunks are no longer written in order.
    /// - A write error surfaces on a later `write` call, or at `close`, rather than
    ///   the call that dispatched it. `skip_limit` therefore applies with a delay of
    ///   up to `concurrency` chunks.
    ///
    /// Only PostgreSQL and MySQL honour this. SQLite serialises writes behind a
    /// database-level lock, so `build_sqlite` ignores the setting.
    ///
    /// Keep this at or below the connection pool size; extra tasks would only queue
    /// waiting for a connection.
    ///
    /// # Examples
    ///
    /// ```
    /// use spring_batch_rs::item::rdbc::RdbcItemWriterBuilder;
    /// use spring_batch_rs::item::rdbc::ColumnValue;
    ///
    /// struct Order { id: i32 }
    ///
    /// let builder = RdbcItemWriterBuilder::<Order>::new()
    ///     .table("orders")
    ///     .column("id", |o: &Order| ColumnValue::Int(o.id as i64))
    ///     .with_concurrency(4);
    ///
    /// let writer = builder.build_postgres();
    /// assert_eq!(writer.concurrency, 4);
    /// ```
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }
```

In `build_postgres` (:252) and `build_mysql` (:291), pass `concurrency: self.concurrency` into the constructed writer. In `build_sqlite` (:330), warn and ignore:

```rust
        if self.concurrency > 1 {
            log::warn!(
                "with_concurrency({}) ignored for SQLite: writes are serialised by a \
                 database-level lock, so concurrency would add no throughput",
                self.concurrency
            );
        }
```

In `postgres_writer.rs`, add `pub(crate) concurrency: usize` to the struct (:43-48) and `concurrency: 1` to `new()` (:52-57). Do the same in `mysql_writer.rs`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --all-features --lib item::rdbc && cargo test --doc --all-features unified_writer_builder`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/item/rdbc/unified_writer_builder.rs src/item/rdbc/postgres_writer.rs src/item/rdbc/mysql_writer.rs
git commit -m "feat: add with_concurrency to RdbcItemWriterBuilder

Carries the setting to the Postgres and MySQL writers; nothing consumes
it yet. SQLite warns and ignores it."
```

---

### Task 4: Concurrent path in the PostgreSQL writer

**Files:**
- Modify: `src/item/rdbc/postgres_writer.rs` — the `write` body (around :95-135) and the `ItemWriter` impl to add `close`
- Test: inline `mod tests` in the same file

**Interfaces:**
- Consumes: `InflightWrites::{new, spawn, harvest, drain}` (Task 2), `writer.concurrency` (Task 3), the fatal `close()` behaviour (Task 1).
- Produces: `PostgresItemWriter` honouring `concurrency`, with `flush` harvesting and `close` draining. Task 5 mirrors this shape for MySQL.

- [ ] **Step 1: Write the failing tests**

The existing tests in this file are builder-state tests that need no database. Add these, which also need none:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn should_take_the_sequential_path_when_concurrency_is_one() {
    let writer = PostgresItemWriter::<String>::new();

    // No pool configured: the sequential path must fail validation immediately
    // rather than dispatching anything.
    let result = writer.write(&["a".to_string()]);

    assert!(result.is_err(), "no pool means validate_config must reject");
    assert_eq!(writer.concurrency, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn should_report_no_error_from_flush_and_close_when_idle() {
    let writer = PostgresItemWriter::<String>::new();

    assert!(ItemWriter::<String>::flush(&writer).is_ok());
    assert!(ItemWriter::<String>::close(&writer).is_ok());
}
```

The real proof that writes overlap lives in `InflightWrites`' own tests (Task 2) and in the benchmark (Task 6). Do not attempt to fake a `sqlx::Pool` here.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --all-features --lib item::rdbc::postgres_writer`
Expected: FAIL to compile — `PostgresItemWriter` has no `close` in its `ItemWriter` impl and no `inflight` field.

- [ ] **Step 3: Write minimal implementation**

Add to the struct in `postgres_writer.rs`:

```rust
    pub(crate) inflight: std::cell::RefCell<Option<InflightWrites>>,
```

and `inflight: std::cell::RefCell::new(None)` to `new()`. Import `use crate::item::rdbc::inflight_writes::InflightWrites;`.

The `Option` is lazy initialisation: the table name is not known until `write` runs.

Restructure `write` so the sequential path is untouched:

```rust
    fn write(&self, items: &[O]) -> ItemWriterResult {
        let (pool, table) = validate_config(
            self.pool.as_ref(),
            self.table.as_deref(),
            self.column_bindings.len(),
        )?;

        if self.concurrency <= 1 {
            return self.write_sequential(pool, table, items);
        }

        self.write_concurrent(pool, table, items)
    }
```

Move the current body verbatim into `write_sequential(&self, pool: &sqlx::Pool<Postgres>, table: &str, items: &[O]) -> ItemWriterResult` — do not alter it while moving.

Add the concurrent path:

```rust
    /// Dispatches the chunk as a task, bounded by `concurrency`.
    ///
    /// Column values are extracted here, on the calling thread, because the
    /// extractor closures are not `Send`. Only the resulting `ColumnValue`s —
    /// which are `Send + 'static` — cross into the spawned task.
    fn write_concurrent(
        &self,
        pool: &sqlx::Pool<Postgres>,
        table: &str,
        items: &[O],
    ) -> ItemWriterResult {
        let rows: Vec<Vec<ColumnValue>> = items
            .iter()
            .map(|item| {
                self.column_bindings
                    .iter()
                    .map(|(_, extractor)| extractor(item))
                    .collect()
            })
            .collect();

        let col_list = self
            .column_bindings
            .iter()
            .map(|(n, _)| n.as_str())
            .collect::<Vec<_>>()
            .join(",");

        let pool = pool.clone();
        let table_owned = table.to_string();

        let mut guard = self.inflight.borrow_mut();
        let inflight = guard.get_or_insert_with(|| {
            InflightWrites::new(self.concurrency, table.to_string(), "PostgreSQL")
        });

        inflight.spawn(async move {
            let mut query_builder = QueryBuilder::new("INSERT INTO ");
            query_builder.push(&table_owned);
            query_builder.push(" (");
            query_builder.push(&col_list);
            query_builder.push(") ");
            // `into_iter` rather than `iter`: bind_column_value! moves the value
            // out of the enum (writer_common.rs:96), so consuming the rows avoids
            // a clone per column on the hot path.
            query_builder.push_values(rows.into_iter(), |mut b, row| {
                for value in row {
                    bind_column_value!(b, value);
                }
            });
            query_builder.build().execute(&pool).await.map(|_| ())
        })
    }
```

Then add `flush` and `close` to the `ItemWriter` impl:

```rust
    /// Collects results of finished writes without waiting.
    ///
    /// Deliberately non-blocking: the step engine calls this after every chunk,
    /// so waiting here would serialise the concurrent path back into sequence.
    fn flush(&self) -> ItemWriterResult {
        match self.inflight.borrow_mut().as_mut() {
            Some(inflight) => inflight.harvest(),
            None => Ok(()),
        }
    }

    /// Waits for every in-flight write and reports the first failure.
    fn close(&self) -> ItemWriterResult {
        match self.inflight.borrow_mut().as_mut() {
            Some(inflight) => inflight.drain(),
            None => Ok(()),
        }
    }
```

If `bind_column_value!` cannot accept an owned `ColumnValue`, read its definition in
`src/item/rdbc/` and adapt the call rather than changing the macro — other writers depend on it.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --all-features --lib item::rdbc && cargo clippy --all-features --lib -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/item/rdbc/postgres_writer.rs
git commit -m "feat: concurrent chunk writes in the PostgreSQL writer"
```

---

### Task 5: Concurrent path in the MySQL writer

**Files:**
- Modify: `src/item/rdbc/mysql_writer.rs` — the `write` body (around :100-140) and the `ItemWriter` impl
- Test: inline `mod tests` in the same file

**Interfaces:**
- Consumes: `InflightWrites` (Task 2), `writer.concurrency` (Task 3).
- Produces: nothing later tasks depend on.

Apply the same shape as Task 4, with `MySql` in place of `Postgres` and `"MySQL"` as the `db_name` passed to `InflightWrites::new`. Read Task 4's implementation before starting — the structure is deliberately identical, and divergence between the two writers is a defect, not a style choice.

- [ ] **Step 1: Write the failing tests**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn should_take_the_sequential_path_when_concurrency_is_one() {
    let writer = MySqlItemWriter::<String>::new();

    let result = writer.write(&["a".to_string()]);

    assert!(result.is_err(), "no pool means validate_config must reject");
    assert_eq!(writer.concurrency, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn should_report_no_error_from_flush_and_close_when_idle() {
    let writer = MySqlItemWriter::<String>::new();

    assert!(ItemWriter::<String>::flush(&writer).is_ok());
    assert!(ItemWriter::<String>::close(&writer).is_ok());
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --all-features --lib item::rdbc::mysql_writer`
Expected: FAIL to compile — no `close` impl, no `inflight` field.

- [ ] **Step 3: Write minimal implementation**

Mirror Task 4 exactly:

1. Add `pub(crate) inflight: std::cell::RefCell<Option<InflightWrites>>` to the struct and `RefCell::new(None)` to `new()`.
2. Split `write` into a `concurrency <= 1` guard calling `write_sequential` (the current body, moved verbatim) and `write_concurrent`.
3. In `write_concurrent`, extract `Vec<Vec<ColumnValue>>` on the calling thread, clone the pool, build the `QueryBuilder::<MySql>` inside the spawned future, and pass `"MySQL"` to `InflightWrites::new`.
4. Add the same `flush` (harvest) and `close` (drain) implementations.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --all-features --lib item::rdbc && cargo clippy --all-features --lib -- -D warnings`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/item/rdbc/mysql_writer.rs
git commit -m "feat: concurrent chunk writes in the MySQL writer"
```

---

### Task 6: Integration test, benchmark wiring, docs, version

**Files:**
- Modify: `tests/rdbc_postgres.rs` (add one testcontainers test)
- Modify: `examples/benchmark_csv_postgres_xml.rs` (read a `WRITE_CONCURRENCY` env var)
- Modify: `Cargo.toml:3` (version), `CLAUDE.md` (version line)
- Modify: `../sbrs-docsite/src/content/docs/reference/performance.mdx`, `../sbrs-docsite/src/content/docs/item-readers-writers/overview.mdx`

**Interfaces:**
- Consumes: everything above.
- Produces: nothing.

- [ ] **Step 1: Add the integration test**

In `tests/rdbc_postgres.rs`, following the existing testcontainers setup in that file:

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn should_write_every_row_with_concurrency() {
    // Reuse this file's existing container + pool + table setup helpers.
    let (_container, pool) = setup_postgres().await;
    create_test_table(&pool).await;

    let items: Vec<TestRecord> = (0..10_000)
        .map(|i| TestRecord { id: i, name: format!("row-{i}") })
        .collect();

    let reader = /* a reader over `items`, as other tests in this file do */;
    let writer = RdbcItemWriterBuilder::<TestRecord>::new()
        .postgres(&pool)
        .table("test_records")
        .column("id", |r: &TestRecord| ColumnValue::Int(r.id as i64))
        .column("name", |r: &TestRecord| ColumnValue::Text(r.name.clone()))
        .with_concurrency(4)
        .build_postgres();

    let step = StepBuilder::new("concurrent-write")
        .chunk(500)
        .reader(&reader)
        .processor(&PassThroughProcessor::<TestRecord>::new())
        .writer(&writer)
        .build();
    let job = JobBuilder::new().start(&step).build();
    job.run().expect("job must succeed");

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM test_records")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(count, 10_000, "concurrent writes must not lose or duplicate rows");
}
```

Match the helper names actually used in `tests/rdbc_postgres.rs` — read the file first; the names above are placeholders for whatever that file already provides.

- [ ] **Step 2: Run the integration test**

Run: `cargo test --all-features --test rdbc_postgres should_write_every_row_with_concurrency`
Requires Docker. If no Docker-compatible runtime is available, report that this step could not run — do not claim it passed.

- [ ] **Step 3: Wire the benchmark**

In `examples/benchmark_csv_postgres_xml.rs`, next to the existing `total_records()` helper:

```rust
/// Number of chunk writes allowed in flight, via `WRITE_CONCURRENCY`. Defaults to 1
/// (sequential), matching the writer's own default.
fn write_concurrency() -> usize {
    env::var("WRITE_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1)
}
```

Apply `.with_concurrency(write_concurrency())` to the two PostgreSQL writers (steps 1 and 3), and add the setting to the module doc-comment's Run section.

- [ ] **Step 4: Measure**

```bash
DATABASE_URL="postgresql://postgres:postgres@127.0.0.1:5432/benchmark" \
  TOTAL_RECORDS=1000000 WRITE_CONCURRENCY=1 \
  cargo run --release --example benchmark_csv_postgres_xml --features csv,xml,rdbc-postgres 2>&1 | grep "Step '"
```

Repeat with `WRITE_CONCURRENCY=2`, `4`, `8`. Record `write_duration` and total per setting in
`docs/superpowers/specs/2026-08-03-concurrent-rdbc-writes-results.md`, one table per setting.

Expect sub-linear gains: PostgreSQL's WAL and primary-key maintenance become the limit. If
concurrency 8 is no better than 4, that is the saturation point and worth recording as the
recommended ceiling.

- [ ] **Step 5: Version and docs**

`Cargo.toml:3` → `version = "0.5.0"`. Update the version line in `CLAUDE.md`.

In `sbrs-docsite/src/content/docs/reference/performance.mdx`, add a section on `with_concurrency`
covering: what it does, that it is opt-in, that ordering is not preserved, that errors are delayed
by up to N chunks, that it should stay at or below the pool size, and the measured numbers from
Step 4. In `item-readers-writers/overview.mdx`, note the option on the RDBC writer entry.
**`.mdx` only — never create a `.md` sibling.**

Also document the `close()` behavioural change: a failing `close()` now fails the step.

- [ ] **Step 6: Verify and commit**

```bash
cargo test --all-features --lib
cargo test --doc --all-features
cargo clippy --all-features --lib -- -D warnings
cargo fmt --all -- --check
```

```bash
git add Cargo.toml CLAUDE.md examples/ tests/ docs/
git commit -m "feat: wire concurrency into the benchmark, bump to 0.5.0"
cd ../sbrs-docsite
git add src/content/docs/reference/performance.mdx src/content/docs/item-readers-writers/overview.mdx
git commit -m "docs: document with_concurrency and the close() change"
```

`sbrs-lib` and `sbrs-docsite` are separate repositories — hence two commits.

---

## Self-Review

**Spec coverage:**

| Spec section | Task |
|---|---|
| API `with_concurrency`, default 1 | Task 3 |
| SQLite warns and stays sequential | Task 3 |
| Scope: PostgreSQL + MySQL only | Tasks 4, 5 |
| Internal structure, `RefCell` | Tasks 3, 4 |
| Non-Send closures applied on calling thread | Task 4 Step 3 (`write_concurrent`) |
| Flow: materialise → harvest → block → spawn → report | Task 2 (`spawn`) + Task 4 |
| `flush()` must not drain | Task 2 (`harvest` non-blocking, test) + Task 4 (`flush`) |
| `close()` drains | Task 2 (`drain`) + Task 4 |
| Errors delayed by ≤ N, documented | Task 3 rustdoc, Task 6 docs |
| Ordering not preserved, documented | Task 3 rustdoc, Task 6 docs |
| Engine: failing `close()` fails the step | Task 1 |
| `open()` unchanged | Task 1 Step 3, explicit |
| Testing: overlap, delayed error, final-chunk failure, sequential equivalence | Tasks 1, 2, 4, 5 |
| Integration: 10k rows land exactly | Task 6 |
| Measurement at 1/2/4/8 | Task 6 |
| Semver 0.5.0 | Task 6 |

No gaps.

**Placeholder scan:** no TBD/TODO. Two spots defer to the codebase rather than inventing names —
the testcontainers helpers in Task 6 Step 1 and the `bind_column_value!` shape in Task 4 — and both
say explicitly to read the file and match what is there. That is direction, not a placeholder.

**Type consistency:** `InflightWrites::{new, spawn, harvest, drain}` are defined in Task 2 and used
under those exact names in Tasks 4 and 5. `concurrency: usize` is introduced in Task 3 and read in
Tasks 4 and 5. `write_sequential` / `write_concurrent` are named identically in Tasks 4 and 5.
`db_name` is `&'static str` throughout, passed as `"PostgreSQL"` and `"MySQL"` to match the strings
the existing `create_write_error` calls already use.
