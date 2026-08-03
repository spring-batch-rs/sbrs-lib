# Step Phase Profiling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record how much of each step's wall-clock time is spent reading, processing, writing and flushing, so the follow-up throughput work can be chosen from measurements instead of assumptions.

**Architecture:** Four cumulative `Duration` fields are added to `StepExecution`. `read_chunk` and `process_chunk` are renamed to `*_inner` and wrapped by timing shims (they have multiple early-return points, so inline timing would miss paths). `write_chunk` is timed inline instead, because `write` and `flush` must be attributed separately. A public `StepExecution::phase_summary()` formats the breakdown and is logged at step completion. Separately, `CsvItemWriter` gains the `open`/`close` lifecycle methods it is missing.

**Tech Stack:** Rust 2024 edition, `std::time::{Instant, Duration}`, `log`, `mockall` 0.14 (dev), existing `mock!` blocks in `src/core/step.rs`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-08-03-step-phase-profiling-design.md`.
- Branch: `feat-step-phase-profiling` (already created, spec already committed as `f8fef1b`).
- **No behavioural change to the engine.** The per-chunk `flush()` at `step.rs:845` is measured, not modified or made optional.
- Timing granularity is **per chunk, never per item**. Wrapping each `reader.read()` would add ~0.3 s of `Instant::now()` overhead on the 10M-row benchmark and contaminate the measurement.
- Version bumps to `0.4.0`: adding `pub` fields to `StepExecution`, which is not `#[non_exhaustive]`, is formally breaking.
- Zero-warning policy: `cargo clippy --all-features -- -D warnings` must pass.
- Never use `println!`; use `log` macros (`CLAUDE.md`).
- Test naming: `should_<behaviour>_<condition>` (`.claude/rules/02-unit-tests.md`).
- Every doc-test must assert something (`.claude/rules/01-rustdoc.md`).
- Website sync is mandatory and docsite pages are **`.mdx`, never `.md`**.

---

### Task 1: Phase duration fields on StepExecution

**Files:**
- Modify: `src/core/step.rs:331-359` (struct), `src/core/step.rs:380-396` (constructor)
- Test: `src/core/step.rs` inline `#[cfg(test)] mod tests`

**Interfaces:**
- Consumes: nothing.
- Produces: `StepExecution::read_duration`, `.process_duration`, `.write_duration`, `.flush_duration`, all `pub` and of type `std::time::Duration`, all initialised to `Duration::ZERO`. Tasks 2-4 accumulate into them.

- [ ] **Step 1: Write the failing test**

Add to the inline `mod tests` in `src/core/step.rs`:

```rust
#[test]
fn should_initialize_phase_durations_to_zero() {
    let step_execution = StepExecution::new("phase-init");

    assert_eq!(step_execution.read_duration, Duration::ZERO);
    assert_eq!(step_execution.process_duration, Duration::ZERO);
    assert_eq!(step_execution.write_duration, Duration::ZERO);
    assert_eq!(step_execution.flush_duration, Duration::ZERO);
}
```

`Duration` needs to be in scope in the test module. If `use std::time::Duration;` is not already imported there, add it.

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --all-features should_initialize_phase_durations_to_zero`
Expected: FAIL — `no field 'read_duration' on type 'StepExecution'`

- [ ] **Step 3: Write minimal implementation**

Add the four fields at the end of the `StepExecution` struct (after `write_error_count`, `step.rs:358`):

```rust
    /// Number of errors encountered during writing
    pub write_error_count: usize,
    /// Cumulative time spent inside `ItemReader::read` calls, summed across all chunks
    pub read_duration: Duration,
    /// Cumulative time spent inside `ItemProcessor::process` calls, summed across all chunks
    pub process_duration: Duration,
    /// Cumulative time spent inside `ItemWriter::write` calls, summed across all chunks
    pub write_duration: Duration,
    /// Cumulative time spent inside `ItemWriter::flush` calls, summed across all chunks
    pub flush_duration: Duration,
}
```

And in `StepExecution::new`, after `write_error_count: 0,`:

```rust
            write_error_count: 0,
            read_duration: Duration::ZERO,
            process_duration: Duration::ZERO,
            write_duration: Duration::ZERO,
            flush_duration: Duration::ZERO,
        }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --all-features --lib core::step`
Expected: PASS, including the 61 pre-existing tests in this module.

- [ ] **Step 5: Commit**

```bash
git add src/core/step.rs
git commit -m "feat: add per-phase duration fields to StepExecution"
```

---

### Task 2: Time the read and process phases

**Files:**
- Modify: `src/core/step.rs:729-772` (`read_chunk`), `src/core/step.rs:785-817` (`process_chunk`)
- Test: `src/core/step.rs` inline `mod tests`

**Interfaces:**
- Consumes: the four fields from Task 1.
- Produces: `read_chunk` and `process_chunk` keep their exact existing signatures and become timing shims. The moved bodies become `read_chunk_inner` and `process_chunk_inner` with identical signatures. No caller changes.

**Why a wrapper and not inline timing:** `read_chunk` returns from four different places (`step.rs:748`, `:753`, `:755`, `:767`). Accumulating at each site would be duplicative and easy to get wrong when a path is added later.

- [ ] **Step 1: Write the failing tests**

Add to the inline `mod tests` in `src/core/step.rs`:

```rust
#[test]
fn should_record_nonzero_read_duration_after_step() {
    let mut reader = MockTestItemReader::default();
    let mut counter = 0u16;
    reader.expect_read().returning(move || {
        std::thread::sleep(Duration::from_millis(20));
        counter += 1;
        if counter > 2 { Ok(None) } else { Ok(sample_car()) }
    });

    let processor = PassThroughProcessor::<Car>::new();

    let mut writer = MockTestItemWriter::default();
    writer.expect_open().returning(|| Ok(()));
    writer.expect_write().returning(|_| Ok(()));
    writer.expect_flush().returning(|| Ok(()));
    writer.expect_close().returning(|| Ok(()));

    let step = StepBuilder::new("read-timing")
        .chunk(10)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let mut step_execution = StepExecution::new("read-timing");
    step.execute(&mut step_execution).unwrap();

    assert!(
        step_execution.read_duration >= Duration::from_millis(40),
        "expected read_duration to cover 3 sleeping reads, got {:?}",
        step_execution.read_duration
    );
}

#[test]
fn should_attribute_duration_to_the_correct_phase() {
    let mut reader = MockTestItemReader::default();
    let mut counter = 0u16;
    reader.expect_read().returning(move || {
        counter += 1;
        if counter > 3 { Ok(None) } else { Ok(sample_car()) }
    });

    let processor = PassThroughProcessor::<Car>::new();

    let mut writer = MockTestItemWriter::default();
    writer.expect_open().returning(|| Ok(()));
    writer.expect_write().returning(|_| {
        std::thread::sleep(Duration::from_millis(50));
        Ok(())
    });
    writer.expect_flush().returning(|| Ok(()));
    writer.expect_close().returning(|| Ok(()));

    let step = StepBuilder::new("phase-attribution")
        .chunk(10)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let mut step_execution = StepExecution::new("phase-attribution");
    step.execute(&mut step_execution).unwrap();

    assert!(
        step_execution.write_duration > step_execution.read_duration,
        "slow writer should dominate: write={:?} read={:?}",
        step_execution.write_duration,
        step_execution.read_duration
    );
    assert!(
        step_execution.write_duration > step_execution.process_duration,
        "slow writer should dominate: write={:?} process={:?}",
        step_execution.write_duration,
        step_execution.process_duration
    );
}
```

Add this helper next to the existing `mock_read` helper (`step.rs:1493`) if no equivalent exists:

```rust
fn sample_car() -> Option<Car> {
    Some(Car {
        year: 2024,
        make: "Renault".to_string(),
        model: "Zoe".to_string(),
        description: "electric".to_string(),
    })
}
```

`PassThroughProcessor` comes from `crate::core::item::PassThroughProcessor` — add it to the test module's imports if absent.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --all-features should_record_nonzero_read_duration_after_step should_attribute_duration_to_the_correct_phase`
Expected: FAIL — both assertions fail because durations stay at `Duration::ZERO`.

- [ ] **Step 3: Write minimal implementation**

Rename the existing `read_chunk` (the whole body at `step.rs:729-772`) to `read_chunk_inner`, keeping its signature and body byte-for-byte, and add this shim immediately above it:

```rust
    fn read_chunk(
        &self,
        step_execution: &mut StepExecution,
    ) -> Result<(Vec<I>, ChunkStatus), BatchError> {
        let start = Instant::now();
        let result = self.read_chunk_inner(step_execution);
        step_execution.read_duration += start.elapsed();
        result
    }
```

Do the same for `process_chunk` (`step.rs:785-817`) — rename the existing method to `process_chunk_inner` and add:

```rust
    fn process_chunk(
        &self,
        step_execution: &mut StepExecution,
        read_items: Vec<I>,
    ) -> Result<Vec<O>, BatchError> {
        let start = Instant::now();
        let result = self.process_chunk_inner(step_execution, read_items);
        step_execution.process_duration += start.elapsed();
        result
    }
```

Keep the existing rustdoc on the public-facing shims and leave the `*_inner` methods undocumented beyond a one-line `// timed by the wrapper above` comment — they are private.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --all-features --lib core::step`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/core/step.rs
git commit -m "feat: time the read and process phases of a chunk step"
```

---

### Task 3: Time the write and flush phases separately

**Files:**
- Modify: `src/core/step.rs:830-860` (`write_chunk`)
- Test: `src/core/step.rs` inline `mod tests`

**Interfaces:**
- Consumes: fields from Task 1.
- Produces: `write_duration` covering only `self.writer.write(...)`, `flush_duration` covering only `self.writer.flush()`. Signature of `write_chunk` unchanged.

**Why inline and not a wrapper:** the two calls must be attributed to different fields, so a single wrapper around the method cannot separate them. Isolating `flush_duration` is the whole point — it is what quantifies the cost of the per-chunk flush.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn should_record_flush_duration_separately_from_write() {
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
    writer.expect_flush().returning(|| {
        std::thread::sleep(Duration::from_millis(30));
        Ok(())
    });
    writer.expect_close().returning(|| Ok(()));

    let step = StepBuilder::new("flush-timing")
        .chunk(10)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let mut step_execution = StepExecution::new("flush-timing");
    step.execute(&mut step_execution).unwrap();

    assert!(
        step_execution.flush_duration >= Duration::from_millis(30),
        "flush_duration should capture the sleeping flush, got {:?}",
        step_execution.flush_duration
    );
    assert!(
        step_execution.flush_duration > step_execution.write_duration,
        "a slow flush must not be attributed to write: flush={:?} write={:?}",
        step_execution.flush_duration,
        step_execution.write_duration
    );
}

#[test]
fn should_leave_write_duration_at_zero_for_empty_chunk() {
    let mut reader = MockTestItemReader::default();
    reader.expect_read().returning(|| Ok(None));

    let processor = PassThroughProcessor::<Car>::new();

    let mut writer = MockTestItemWriter::default();
    writer.expect_open().returning(|| Ok(()));
    writer.expect_close().returning(|| Ok(()));

    let step = StepBuilder::new("empty-chunk")
        .chunk(10)
        .reader(&reader)
        .processor(&processor)
        .writer(&writer)
        .build();

    let mut step_execution = StepExecution::new("empty-chunk");
    step.execute(&mut step_execution).unwrap();

    assert_eq!(
        step_execution.write_duration,
        Duration::ZERO,
        "the empty-chunk early return skips the writer entirely"
    );
    assert_eq!(step_execution.flush_duration, Duration::ZERO);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --all-features should_record_flush_duration_separately_from_write should_leave_write_duration_at_zero_for_empty_chunk`
Expected: `should_record_flush_duration_separately_from_write` FAILS (both durations are zero). `should_leave_write_duration_at_zero_for_empty_chunk` may already pass — that is fine, it is a regression guard for the early return at `step.rs:837-840`.

- [ ] **Step 3: Write minimal implementation**

Replace the `match` block in `write_chunk` (`step.rs:842-859`) with:

```rust
        let write_start = Instant::now();
        let write_result = self.writer.write(processed_items);
        step_execution.write_duration += write_start.elapsed();

        match write_result {
            Ok(()) => {
                step_execution.write_count += processed_items.len();

                let flush_start = Instant::now();
                let flush_result = self.writer.flush();
                step_execution.flush_duration += flush_start.elapsed();
                Self::manage_error(flush_result);

                Ok(())
            }
            Err(error) => {
                warn!("Error writing items: {}", error);
                step_execution.write_error_count += processed_items.len();

                if self.is_skip_limit_reached(step_execution) {
                    // Set the status to WriteError to indicate a write failure
                    step_execution.status = StepStatus::WriteError;
                    return Err(error);
                }
                Ok(())
            }
        }
```

Leave the empty-chunk early return at `step.rs:837-840` untouched — it must keep returning before any writer call.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --all-features --lib core::step`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/core/step.rs
git commit -m "feat: time write and flush phases separately"
```

---

### Task 4: Phase summary formatting and logging

**Files:**
- Modify: `src/core/step.rs:361-397` (`impl StepExecution`), `src/core/step.rs:658-661` (end of `ChunkOrientedStep::execute`)
- Test: `src/core/step.rs` inline `mod tests`

**Interfaces:**
- Consumes: all four duration fields, plus the existing `duration: Option<Duration>` and `name: String`.
- Produces: `pub fn phase_summary(&self) -> String` on `StepExecution`. Public because the point of this work is letting a user read their own measurements.

- [ ] **Step 1: Write the failing tests**

```rust
#[test]
fn should_format_phase_summary_with_percentages() {
    let mut step_execution = StepExecution::new("load-postgres");
    step_execution.duration = Some(Duration::from_secs(10));
    step_execution.read_duration = Duration::from_secs(5);
    step_execution.process_duration = Duration::from_secs(1);
    step_execution.write_duration = Duration::from_secs(3);
    step_execution.flush_duration = Duration::from_secs(1);

    let summary = step_execution.phase_summary();

    assert!(summary.contains("load-postgres"), "summary: {summary}");
    assert!(summary.contains("read 5.0s (50%)"), "summary: {summary}");
    assert!(summary.contains("process 1.0s (10%)"), "summary: {summary}");
    assert!(summary.contains("write 3.0s (30%)"), "summary: {summary}");
    assert!(summary.contains("flush 1.0s (10%)"), "summary: {summary}");
}

#[test]
fn should_report_zero_percentages_when_duration_is_unset() {
    let step_execution = StepExecution::new("never-ran");

    let summary = step_execution.phase_summary();

    assert!(summary.contains("(0%)"), "summary: {summary}");
    assert!(!summary.contains("NaN"), "division by zero leaked: {summary}");
    assert!(!summary.contains("inf"), "division by zero leaked: {summary}");
}
```

The second test is the one that matters: `duration` is `Option<Duration>` and is `None` until `execute` finishes, so a naive percentage computation yields `NaN`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --all-features should_format_phase_summary_with_percentages should_report_zero_percentages_when_duration_is_unset`
Expected: FAIL — `no method named 'phase_summary'`.

- [ ] **Step 3: Write minimal implementation**

Add to `impl StepExecution` (after `new`, `step.rs:396`):

```rust
    /// Formats the per-phase timing breakdown as a single human-readable line.
    ///
    /// Percentages are relative to the step's total [`StepExecution::duration`].
    /// When `duration` is `None` or zero — for instance before the step has run —
    /// every percentage is reported as `0%` rather than `NaN`.
    ///
    /// The four phases will not sum to exactly 100%: the remainder is framework
    /// overhead. A large remainder is itself a useful signal.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use spring_batch_rs::core::step::StepExecution;
    /// use std::time::Duration;
    ///
    /// let mut step_execution = StepExecution::new("load-postgres");
    /// step_execution.duration = Some(Duration::from_secs(10));
    /// step_execution.read_duration = Duration::from_secs(5);
    ///
    /// let summary = step_execution.phase_summary();
    /// assert!(summary.contains("read 5.0s (50%)"));
    /// ```
    pub fn phase_summary(&self) -> String {
        let total = self.duration.unwrap_or_default().as_secs_f64();
        let pct = |d: Duration| -> f64 {
            if total > 0.0 {
                d.as_secs_f64() / total * 100.0
            } else {
                0.0
            }
        };

        format!(
            "Step '{}' {:.1}s — read {:.1}s ({:.0}%) | process {:.1}s ({:.0}%) | write {:.1}s ({:.0}%) | flush {:.1}s ({:.0}%)",
            self.name,
            total,
            self.read_duration.as_secs_f64(),
            pct(self.read_duration),
            self.process_duration.as_secs_f64(),
            pct(self.process_duration),
            self.write_duration.as_secs_f64(),
            pct(self.write_duration),
            self.flush_duration.as_secs_f64(),
            pct(self.flush_duration),
        )
    }
```

Then in `ChunkOrientedStep::execute`, immediately **after** the duration assignment at `step.rs:661` (it must come after, because `phase_summary` reads `duration`):

```rust
        step_execution.duration = Some(start_time.elapsed());

        info!("{}", step_execution.phase_summary());
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --all-features --lib core::step && cargo test --doc --all-features core::step`
Expected: PASS, including the new doc-test.

- [ ] **Step 5: Commit**

```bash
git add src/core/step.rs
git commit -m "feat: log per-phase timing summary at step completion"
```

---

### Task 5: CsvItemWriter open/close lifecycle

**Files:**
- Modify: `src/item/csv/csv_writer.rs` (the `impl ItemWriter<O> for CsvItemWriter` block, after `flush` at `:197-204`)
- Test: `src/item/csv/csv_writer.rs` inline `#[cfg(test)] mod tests`

**Interfaces:**
- Consumes: the existing `CsvItemWriter::flush` (`csv_writer.rs:197`).
- Produces: `open`/`close` on `CsvItemWriter`, `close` delegating to `flush`.

**Why:** `CsvItemWriter` implements only `write` and `flush`, inheriting the no-op `open`/`close` defaults from `ItemWriter` (`item.rs:204`, `:220`). `JsonItemWriter` and `XmlItemWriter` both flush in `close` (`json_writer.rs:174`, `xml_writer.rs:144`); CSV is the outlier. Today the per-chunk `flush()` at `step.rs:845` is the only thing guaranteeing CSV rows reach disk before the job ends — `csv::Writer` does flush on `Drop`, but that runs after `job.run()` returns and silently discards the error. This is a robustness fix on its own merits, and it is the precondition for ever making the per-chunk flush optional.

- [ ] **Step 1: Write the failing tests**

Add to the existing inline `mod tests` in `src/item/csv/csv_writer.rs` (it starts at `csv_writer.rs:494` — note that `.claude/rules/02-unit-tests.md` still lists this file as having no tests, which is stale):

```rust
#[test]
fn should_flush_pending_rows_on_close() {
    #[derive(Serialize)]
    struct Row {
        name: String,
        value: u32,
    }

    let mut buffer = Vec::new();
    {
        let writer = CsvItemWriterBuilder::<Row>::new()
            .has_headers(true)
            .from_writer(&mut buffer);

        writer
            .write(&[Row { name: "alpha".to_string(), value: 1 }])
            .unwrap();

        // close() must make the data durable without relying on Drop
        ItemWriter::<Row>::close(&writer).unwrap();
    }

    let output = String::from_utf8(buffer).unwrap();
    assert!(output.contains("alpha,1"), "close did not flush: {output}");
}

#[test]
fn should_return_ok_from_open() {
    #[derive(Serialize)]
    struct Row {
        name: String,
    }

    let mut buffer = Vec::new();
    let writer = CsvItemWriterBuilder::<Row>::new().from_writer(&mut buffer);

    assert!(ItemWriter::<Row>::open(&writer).is_ok());
}
```

Note the test asserts on `buffer` **inside** a scope that ends before the read, but `close()` is called before the scope ends — that ordering is what proves `close` flushed rather than `Drop`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --all-features --lib item::csv::csv_writer`
Expected: `should_flush_pending_rows_on_close` FAILS — the default no-op `close` writes nothing, so the buffer is empty at assertion time.

- [ ] **Step 3: Write minimal implementation**

Add to the `impl<O: Serialize, W: Write> ItemWriter<O> for CsvItemWriter<O, W>` block, after `flush`:

```rust
    /// Prepares the writer. CSV has no header ceremony to emit here — headers are
    /// written lazily by the underlying `csv::Writer` on the first record — so this
    /// is an explicit no-op provided for symmetry with the JSON and XML writers.
    ///
    /// # Returns
    /// - `Ok(())` always
    ///
    /// # Examples
    ///
    /// ```
    /// use spring_batch_rs::item::csv::csv_writer::CsvItemWriterBuilder;
    /// use spring_batch_rs::core::item::ItemWriter;
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct Record { id: u32 }
    ///
    /// let mut buffer = Vec::new();
    /// let writer = CsvItemWriterBuilder::<Record>::new().from_writer(&mut buffer);
    /// assert!(ItemWriter::<Record>::open(&writer).is_ok());
    /// ```
    fn open(&self) -> ItemWriterResult {
        Ok(())
    }

    /// Finalizes the CSV output by flushing all buffered records.
    ///
    /// Without this, buffered rows would only reach the destination when the
    /// underlying `csv::Writer` is dropped, which discards any I/O error.
    ///
    /// # Returns
    /// - `Ok(())` if all buffered data was written
    /// - `Err(BatchError::ItemWriter)` if flushing the underlying writer failed
    ///
    /// # Examples
    ///
    /// ```
    /// use spring_batch_rs::item::csv::csv_writer::CsvItemWriterBuilder;
    /// use spring_batch_rs::core::item::ItemWriter;
    /// use serde::Serialize;
    ///
    /// #[derive(Serialize)]
    /// struct Record { id: u32 }
    ///
    /// let mut buffer = Vec::new();
    /// {
    ///     let writer = CsvItemWriterBuilder::<Record>::new().from_writer(&mut buffer);
    ///     writer.write(&[Record { id: 7 }]).unwrap();
    ///     ItemWriter::<Record>::close(&writer).unwrap();
    /// }
    /// assert!(String::from_utf8(buffer).unwrap().contains('7'));
    /// ```
    fn close(&self) -> ItemWriterResult {
        self.flush()
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --all-features --lib item::csv && cargo test --doc --all-features item::csv`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/item/csv/csv_writer.rs
git commit -m "fix: flush CsvItemWriter on close

CsvItemWriter implemented neither open nor close, inheriting the no-op
defaults, so the per-chunk flush in the step engine was the only thing
guaranteeing rows reached disk before a job ended."
```

---

### Task 6: Version bump and documentation sync

**Files:**
- Modify: `Cargo.toml:3` (version), `CLAUDE.md` (version line)
- Modify: `../sbrs-docsite/src/content/docs/reference/performance.mdx`
- Modify: `../sbrs-docsite/src/content/docs/api/item-writer.mdx`

**Interfaces:**
- Consumes: `StepExecution::phase_summary` (Task 4), `CsvItemWriter::close` (Task 5).
- Produces: nothing consumed by later tasks.

- [ ] **Step 1: Bump the version**

In `Cargo.toml:3`, change `version = "0.3.6"` to `version = "0.4.0"`.

Adding `pub` fields to `StepExecution`, which is not `#[non_exhaustive]`, breaks downstream struct-literal construction. In practice `StepExecution::new` is the only construction path, but the change is formally breaking.

Also update the `**Version**: 0.3.0` line near the top of `CLAUDE.md` to `0.4.0` — it is already stale.

- [ ] **Step 2: Document the phase breakdown on the docsite**

In `sbrs-docsite/src/content/docs/reference/performance.mdx`, add a section explaining how to read the breakdown. **`.mdx` only — never create a `.md` alongside it, the `.md` would shadow the `.mdx`.**

````mdx
## Reading the per-phase breakdown

Every chunk-oriented step logs where its time went:

```text
Step 'load-postgres' 42.3s — read 18.1s (43%) | process 2.4s (6%) | write 21.1s (50%) | flush 0.7s (2%)
```

The same numbers are available programmatically:

```rust title="reading step metrics"
let job = JobBuilder::new().start(&step).build();
job.run()?;

let metrics = job.get_step_execution("load-postgres").unwrap();
println!("{}", metrics.phase_summary());
println!("read: {:?}", metrics.read_duration);
```

Percentages are relative to the step's total duration. The four phases do not sum
to 100% — the remainder is framework overhead, and a large remainder is itself
worth investigating.

Timing is recorded once per chunk, not once per item, so the overhead stays
negligible even on multi-million-row jobs.
````

- [ ] **Step 3: Document the close() durability contract**

In `sbrs-docsite/src/content/docs/api/item-writer.mdx`, add to the lifecycle section:

```mdx
`close()` is where file-based writers guarantee durability. `CsvItemWriter`,
`JsonItemWriter` and `XmlItemWriter` all flush buffered data there. If you
implement a custom buffered `ItemWriter`, flush in `close()` — do not rely on
`Drop`, which cannot report an I/O error.
```

- [ ] **Step 4: Verify everything**

```bash
make dev
cargo test --doc --all-features
cargo clippy --all-features -- -D warnings
```

Expected: all green. `make dev` runs format, lint and the full test suite (348 inline + 89 integration tests).

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml CLAUDE.md
git commit -m "chore: bump to 0.4.0 for StepExecution field additions"
cd ../sbrs-docsite
git add src/content/docs/reference/performance.mdx src/content/docs/api/item-writer.mdx
git commit -m "docs: document per-phase step timing and close() durability"
```

Note: `sbrs-lib` and `sbrs-docsite` are **separate git repositories**, hence the two commits.

---

### Task 7: Run the measurement

**Files:**
- Read only: `examples/benchmark_csv_postgres_xml.rs`
- Create: `docs/superpowers/specs/2026-08-03-step-phase-profiling-results.md`

**Interfaces:**
- Consumes: everything above.
- Produces: the measurement table that selects the follow-up work.

- [ ] **Step 1: Run the benchmark at three scales**

Requires Docker (the example uses a PostgreSQL container).

```bash
cargo run --release --example benchmark_csv_postgres_xml \
  --features csv,xml,rdbc-postgres 2>&1 | grep "^Step"
```

Run at **100k, 1M and 10M rows** — check how the example parameterises row count and vary it. Three scales expose non-linear behaviour, such as `LIMIT/OFFSET` pagination degrading at large offsets (which is why keyset pagination exists in the readers).

- [ ] **Step 2: Record the results**

Write `docs/superpowers/specs/2026-08-03-step-phase-profiling-results.md` with one table per scale, one row per step:

```markdown
| Step | Total | read | process | write | flush | overhead |
|---|---|---|---|---|---|---|
| csv-to-postgres | | | | | | |
| postgres-to-xml | | | | | | |
| xml-to-postgres | | | | | | |
```

Record all three steps separately — they have very different profiles and averaging them hides the answer.

- [ ] **Step 3: Apply the decision table**

From the spec, verbatim:

| Observation | Follow-up |
|---|---|
| `read` dominates (waiting on PostgreSQL / CSV) | **B** — prefetch page N+1 in the RDBC/ORM readers |
| `write` dominates (PostgreSQL INSERT) | **B** — write-behind in the RDBC writers |
| All three phases the same order of magnitude | **A** — stage pipelining; only option that overlaps them |
| `process` dominates (XML serialisation) | **Neither A nor B** — CPU parallelism (`rayon`), not async |
| `flush` > 5% | Make the per-chunk flush optional on `StepBuilder` |

State the conclusion explicitly at the end of the results document, then stop. The follow-up work gets its own brainstorm and spec.

- [ ] **Step 4: Commit**

```bash
git add docs/superpowers/specs/2026-08-03-step-phase-profiling-results.md
git commit -m "docs: record step phase profiling measurements"
```

---

## Self-Review

**Spec coverage:**

| Spec section | Task |
|---|---|
| Instrumentation — four fields | Task 1 |
| Granularity: chunk not item | Tasks 2-3 (wrapper + inline, never per-item) |
| Attribution points table | Tasks 2 (read, process) and 3 (write, flush) |
| Reporting — `info!` line + programmatic access | Task 4 |
| CsvItemWriter lifecycle fix | Task 5 |
| Non-goal: per-chunk flush untouched | Task 3 Step 3 leaves `step.rs:837-840` and the flush call itself intact |
| Measurement protocol — 3 scales, 3 steps | Task 7 |
| Decision table | Task 7 Step 3 |
| Semver 0.4.0 | Task 6 Step 1 |
| Testing — 4 step tests + 2 csv tests | Tasks 1-5 (6 step-level tests written, exceeding the spec's 4) |
| Documentation sync | Task 6 |

No gaps.

**Placeholder scan:** no TBD/TODO, no "add error handling", no "similar to Task N". Every code step contains the actual code. The only intentionally blank content is the results table in Task 7, which is a data-collection form, not a placeholder.

**Type consistency:** `read_duration` / `process_duration` / `write_duration` / `flush_duration` are named identically in Tasks 1, 2, 3, 4, 6 and 7. `phase_summary()` is defined in Task 4 and referenced in Task 6 with the same name and `-> String` signature. `read_chunk_inner` / `process_chunk_inner` are introduced in Task 2 and referenced nowhere else. `sample_car()` is defined in Task 2 and reused in Task 3.
