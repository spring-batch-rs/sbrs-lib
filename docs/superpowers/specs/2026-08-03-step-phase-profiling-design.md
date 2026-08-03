# Step Phase Profiling Design

**Date:** 2026-08-03
**Branch:** feat-step-phase-profiling
**Status:** Approved

## Summary

Add per-phase timing (`read` / `process` / `write` / `flush`) to `StepExecution`, report it at
step completion, and fix the `CsvItemWriter` lifecycle gap that the timing work exposes.

This is step 0 of a performance investigation. The motivating question was whether to migrate
the crate to an async API for throughput. The answer cannot be settled without knowing where
time is actually spent today, and no such measurement exists: there is no `benches/` directory,
no criterion dependency, and `StepExecution` counts items but times nothing below the step level.

---

## Background: why measurement comes first

The crate is often described as "fully synchronous", but that is a facade. `tokio` is a
**non-optional** dependency with `features = ["full"]` (`Cargo.toml:37`), and twelve sites bridge
sync to async:

```rust
// src/item/rdbc/postgres_reader.rs:109
tokio::task::block_in_place(|| {
    tokio::runtime::Handle::current().block_on(async { ... })
})
```

Sites: the three RDBC readers and three RDBC writers, the ORM reader and writer, and the four S3
tasklets.

Converting the core traits to `async fn` would **not** improve throughput on its own, and would
likely hurt it:

- `ChunkOrientedStep` holds `&'a dyn ItemReader<I>` (`step.rs:594`) and `JobInstance` holds
  `Vec<&'a dyn Step>` (`job.rs:105`). `async fn` in traits is not dyn-compatible, so preserving
  this design requires `#[async_trait]`, which boxes a future **per call**. `read()` is called
  once per item — 10M heap allocations on the reference benchmark.
- The alternative (generic traits returning `impl Future`) breaks `Vec<&dyn Step>` and therefore
  heterogeneous job composition.
- A chunk pipeline is sequential by construction: `read → process → write → read`. Async only
  buys throughput where it enables **overlap**, which is a separate design decision that does not
  require changing the trait signatures.

The real throughput levers are therefore (A) pipelining the three phases across stages so cost
becomes `max(read, process, write)` instead of their sum, or (B) adding concurrency inside the
adapters (prefetch page N+1, write-behind). Choosing between them requires knowing the phase
breakdown. Hence this spec.

---

## Instrumentation

Four fields added to `StepExecution` (`src/core/step.rs:331-359`):

```rust
pub read_duration: Duration,
pub process_duration: Duration,
pub write_duration: Duration,
pub flush_duration: Duration,
```

All initialised to `Duration::ZERO` in `StepExecution::new` (`step.rs:380-396`). `Instant` and
`Duration` are already imported in this module.

### Granularity: chunk, not item

Timing wraps the loop inside `read_chunk` / `process_chunk` / `write_chunk`, **not** each
individual `reader.read()` or `processor.process()` call.

On 10M rows with `chunk_size = 1000` this is 10,000 measurements instead of 10,000,000. Timing
each item would add roughly 20-30 ns per call — about 0.3 s of pure `Instant::now()` overhead on
the reference benchmark, which would contaminate the very numbers being collected.

### Attribution points

| Field | Location | Wraps |
|---|---|---|
| `read_duration` | `read_chunk`, `step.rs:729-772` | the inner `loop` calling `self.reader.read()` |
| `process_duration` | `process_chunk`, `step.rs:785-817` | the per-item `self.processor.process(item)` loop |
| `write_duration` | `write_chunk`, `step.rs:842` | `self.writer.write(processed_items)` only |
| `flush_duration` | `write_chunk`, `step.rs:845` | `self.writer.flush()` only |

`flush` is measured separately from `write` deliberately: it is what quantifies the cost of the
per-chunk flush discussed below.

The early return for empty chunks (`step.rs:837-840`) records nothing, since no writer call is
made.

---

## Reporting

An `info!` line emitted at the end of `ChunkOrientedStep::execute`, near the existing timing
assignment at `step.rs:659-661`:

```
Step 'load-postgres' 42.3s — read 18.1s (43%) | process 2.4s (6%) | write 21.1s (50%) | flush 0.7s (2%)
```

Percentages are relative to the step's total `duration`. The residual (total minus the four
phases) is framework overhead and is expected to be small; a large residual is itself a finding.

Programmatic access already exists through `JobInstance::get_step_execution(&self, name: &str)`
(`job.rs:213-215`), which returns a cloned `StepExecution` — the new fields come along for free.

---

## CsvItemWriter lifecycle fix

`CsvItemWriter` implements only `write` (`csv_writer.rs:133`) and `flush` (`csv_writer.rs:197`).
It does **not** implement `open` or `close`, so it inherits the no-op defaults from
`ItemWriter` (`item.rs:204`, `item.rs:220`).

`JsonItemWriter` and `XmlItemWriter` both flush in `close()` (`json_writer.rs:174`,
`xml_writer.rs:144`). CSV is the outlier.

Consequence: the per-chunk `flush()` at `step.rs:845` is currently the **only** thing guaranteeing
CSV rows reach disk before the job ends. (`csv::Writer` does flush in its `Drop` impl, but that
runs after `job.run()` returns and silently discards errors.) Removing the per-chunk flush without
fixing this would cause silent data loss.

Fix: implement `close()` on `CsvItemWriter`, delegating to the existing `flush()`. `open()` is
implemented as an explicit no-op returning `Ok(())` for symmetry with the other file writers.

This is a robustness fix that stands on its own merits, independent of the profiling work.

---

## Explicit non-goals

**No behavioural change to the engine in this spec.** In particular, the per-chunk `flush()` at
`step.rs:845` stays exactly as it is. It is measured, not modified. Deciding whether to make it
optional is downstream of the numbers this work produces.

No async conversion, no pipelining, no adapter concurrency. Those are separate specs, gated on the
decision table below.

---

## Measurement protocol

Target: `examples/benchmark_csv_postgres_xml.rs` — already `#[tokio::main]`, one job with three
steps (CSV → PostgreSQL, PostgreSQL → XML, XML → PostgreSQL), `chunk_size = 1000`, pool size 10.

Run at three scales — **100k / 1M / 10M rows** — to expose non-linear behaviour (for example
`LIMIT/OFFSET` pagination degrading quadratically at large offsets, which is why keyset pagination
exists in the readers).

Record the four-phase breakdown for **each of the three steps** separately; they have very
different profiles and averaging them would hide the answer.

---

## Decision table

Fixed in advance so the numbers, not intuition, select the follow-up work.

| Observation | Follow-up |
|---|---|
| `read` dominates (waiting on PostgreSQL / CSV) | **B** — prefetch page N+1 in the RDBC/ORM readers |
| `write` dominates (PostgreSQL INSERT) | **B** — write-behind in the RDBC writers |
| All three phases the same order of magnitude | **A** — stage pipelining; only option that overlaps them |
| `process` dominates (XML serialisation) | **Neither A nor B** — CPU parallelism (`rayon`), not async |
| `flush` > 5% | Make the per-chunk flush optional on `StepBuilder` |

The fourth row matters: there is a plausible outcome where async answers nothing, and it is
cheaper to discover that now.

---

## Semver

Adding `pub` fields to `StepExecution`, which is not `#[non_exhaustive]`, breaks any downstream
struct-literal construction. In practice `StepExecution::new` is the only construction path, but
the change is formally breaking — target **0.4.0**.

---

## Testing

Inline `#[cfg(test)]` in `src/core/step.rs`, following `.claude/rules/02-unit-tests.md`:

- `should_record_nonzero_read_duration_after_step` — mock reader with a deliberate delay, assert
  `read_duration > Duration::ZERO`.
- `should_attribute_duration_to_the_correct_phase` — a slow mock **writer** with fast reader and
  processor; assert `write_duration` exceeds both `read_duration` and `process_duration`.
- `should_record_flush_duration_separately_from_write` — mock writer where `flush` sleeps and
  `write` does not; assert both are non-zero and distinct.
- `should_leave_durations_at_zero_for_empty_chunk` — reader returning `None` immediately; assert
  `write_duration == Duration::ZERO` (the empty-chunk early return skips the writer).

The existing `mock!` blocks at `step.rs:1454-1478` (`MockTestItemReader`, `MockTestProcessor`,
`MockTestItemWriter`) are reused; no new mocking infrastructure is needed.

Inline `#[cfg(test)]` in `src/item/csv/csv_writer.rs`:

- `should_flush_pending_rows_on_close` — write rows, call `close()`, assert the underlying buffer
  contains them **without** relying on drop.
- `should_return_ok_from_open` — assert the no-op contract.

---

## Documentation sync

Per `.claude/rules/04-documentation.md` and the mandatory website-sync rule in `CLAUDE.md`:

- Rustdoc on the four new `StepExecution` fields, including the unit of measurement and the fact
  that they are cumulative across chunks.
- Rustdoc on `CsvItemWriter::open` / `close`.
- `sbrs-docsite/src/content/docs/reference/performance.mdx` — document the new phase breakdown and
  how to read the step summary log. **`.mdx` only, never `.md`.**
- `sbrs-docsite/src/content/docs/api/item-writer.mdx` — note that `close()` is where file writers
  guarantee durability.

---

## Verification

```bash
make dev                                    # format, lint, test
cargo test --all-features                   # 348 inline + 89 integration tests
cargo test --doc --all-features             # ~247 doc blocks still compile
cargo clippy --all-features -- -D warnings  # zero-warning policy
```

End-to-end, with Docker running:

```bash
cargo run --release --example benchmark_csv_postgres_xml \
  --features csv,xml,rdbc-postgres 2>&1 | grep "Step '"
```

Expected: three summary lines, one per step, of the shape

```text
Step 'csv-to-postgres' 42.3s — read 18.1s (43%) | process 2.4s (6%) | write 21.1s (50%) | flush 0.7s (2%)
```

with the four phases summing to somewhat less than the reported step duration (see the residual
note above).

The benchmark prints these itself, on stderr, via `StepExecution::phase_summary()`. Do **not**
rely on the `info!` summary the engine emits at step completion: `env_logger` defaults to level
`error` when `RUST_LOG` is unset, so that record is suppressed, and when it is enabled the
`[timestamp LEVEL target]` prefix means an anchored `^Step` pattern will never match. To see the
engine's own log line instead, run with `RUST_LOG=info`.
