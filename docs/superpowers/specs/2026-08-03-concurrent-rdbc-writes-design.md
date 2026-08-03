# Concurrent RDBC Writes Design

**Date:** 2026-08-03
**Branch:** feat-concurrent-rdbc-writes
**Status:** Approved

## Summary

Add opt-in concurrent chunk writing to the PostgreSQL and MySQL item writers, bounded by a
caller-supplied degree of concurrency. Writes are dispatched to a `JoinSet` of at most N in-flight
tokio tasks; errors are harvested on subsequent `write()` calls and drained at `close()`.

Also fix an engine defect the work exposes: a failing `ItemWriter::close()` is currently swallowed
as a warning, so a step can report success while data was never written.

## Background: why this, and why not async traits

The phase profiling in `2026-08-03-step-phase-profiling-design.md` measured where a step's time
goes. At 10M rows:

| Step | Total | read | process | write | flush |
|---|---|---|---|---|---|
| csv-to-postgres | 57.6s | 15% | 1% | **82%** | 0% |
| postgres-to-xml | 30.8s | **78%** | 0% | 20% | 0% |
| xml-to-postgres-import | 74.0s | 33% | 0% | **66%** | 0% |

Two conclusions drove this design:

- Migrating the traits to async was rejected: time is spent *waiting* on the database, and making
  `write()` async changes who waits, not whether anyone waits.
- Overlapping read against write caps at 26%, because each step's floor becomes its dominant
  phase. For steps 1 and 3 that floor is PostgreSQL write time — 47.3s and 48.9s, about 210k
  rows/s. **Cutting the dominant phase itself is the larger prize**, and that means issuing
  several INSERTs at once.

The writers currently issue one INSERT per chunk, strictly sequentially, while the pool holds 10
connections used one at a time. With `chunk_size = 1000` and 8 columns,
`max_items_per_batch = 65535 / 8 = 8191`, so the existing `items.chunks(max_items)` loop in
`postgres_writer.rs:110` never splits — it is exactly one INSERT per `write()` call, 10 000 of them
in series at ~4.7ms each.

## API

```rust
let writer = RdbcItemWriterBuilder::<Transaction>::new()
    .postgres(pool.clone())
    .table("transactions")
    .with_concurrency(4)      // new; default 1
    .build_postgres();
```

Opt-in. `with_concurrency(1)`, or never calling it, takes **the existing code path unchanged** —
not an equivalent path, the same one. Nobody inherits the reordering or the deferred error timing
without asking for it.

`build_sqlite()` with a concurrency above 1 emits a `warn!` and proceeds sequentially. SQLite
serialises writes behind a database-level lock; an option that silently does nothing would be
worse than no option.

Scope is **PostgreSQL and MySQL only**. SeaORM and MongoDB are out of scope for this spec.

## Internal structure

```rust
pub struct PostgresItemWriter<O> {
    pool: Option<Pool<Postgres>>,
    table: Option<String>,
    column_bindings: Vec<(String, Box<dyn Fn(&O) -> ColumnValue>)>,
    concurrency: usize,
    inflight: RefCell<JoinSet<Result<(), sqlx::Error>>>,
    pending_errors: RefCell<Vec<BatchError>>,
}
```

`RefCell` follows the pattern used throughout the crate, forced by `&self` on `ItemWriter`.

### Why the non-Send closures are not a blocker

`column_bindings` holds `Box<dyn Fn(&O) -> ColumnValue>` with no `Send` bound, which would
normally prevent moving anything that touches them into a spawned task.

It does not, because `ColumnValue` (`column_value.rs:21`) holds only `i64`, `f64`, `String`,
`bool`, `Vec<u8>` and a unit variant — it is `Send + 'static`. The closures are therefore applied
**synchronously inside `write()`**, on the calling thread, and only the resulting values cross the
task boundary. The closures themselves never move.

## Flow of `write()`

With `concurrency <= 1`, dispatch straight to the current implementation and stop.

Otherwise:

1. **Materialise** `Vec<Vec<ColumnValue>>` by applying the bindings to each item.
2. **Harvest** finished tasks without blocking, collecting errors into `pending_errors`.
3. **If N tasks are in flight**, block on `join_next()` until a slot frees. This is the
   backpressure — structural, not a counter to maintain.
4. **Spawn** the INSERT with a cloned `Pool` (`Send + Sync + Clone`), the table name, and the
   column list.
5. **Return** the first pending error, if any, so the engine can count it.

The materialisation in step 1 is a real cost the current code avoids: `push_values`
(`postgres_writer.rs:117`) binds straight from the items with no intermediate buffer. It is
accepted because `process` measures 0-1% — there is CPU headroom, and it buys against a phase at
82%.

## flush() must not drain

The engine calls `flush()` after **every chunk** (`step.rs:955`). A `flush()` that waited for
in-flight tasks would collapse the design back to sequential execution — concurrent code for no
gain.

So `flush()` harvests finished tasks only; it never waits. **`close()` is the only draining
point.**

This does not weaken any existing guarantee: the RDBC writers inherit the trait's no-op `flush()`
today, which is exactly why `flush_duration` measured 0% at every scale.

## Errors and skip_limit

The engine does `write_error_count += processed_items.len()` (`step.rs:963`) — the size of the
*current* chunk, while the error came from a chunk dispatched up to N positions earlier. Counts
stay right in magnitude, but attribution is approximate and `skip_limit` applies with a delay
bounded by N.

This is a documented consequence of opting in, not a defect to paper over. It belongs in the
rustdoc for `with_concurrency` and on the website's performance page.

Chunk write **ordering is not preserved** under concurrency. For plain INSERTs this is harmless;
for schemas with ordering dependencies it is not, which is another reason the feature is opt-in.

## Engine fix: a failing close() must fail the step

`ChunkOrientedStep::execute` calls `Self::manage_error(self.writer.close())` (`step.rs:729`), and
`manage_error` (`step.rs:988`) only logs a warning. The error is discarded.

With writes in flight, the final batch's outcome is knowable only at `close()`. Discarding it
would let a step report `Success` while INSERTs failed — silent data loss.

**Change:** a failing `close()` sets `StepStatus::WriteError` and makes the step return `Err`.

This is a pre-existing defect independent of concurrency — a failing final CSV flush is swallowed
the same way today. Fixing it is an improvement in its own right, but it is an observable
behaviour change: a job that previously "passed" with a close error will now fail. That is the
correct outcome, and it must be called out in the changelog.

`open()` keeps its current non-fatal handling. Nothing in this design depends on changing it, and
widening the blast radius here would serve nothing.

## Non-goals

- No async migration of the `ItemReader` / `ItemWriter` / `Tasklet` traits.
- No prefetch in the readers. That is a separate, complementary change.
- No stage pipelining in the engine.
- No change to the per-chunk `flush()` call — measured at 0%, it stays.
- No concurrency for SeaORM, MongoDB, or SQLite writers.

## Testing

Unit tests, inline per `.claude/rules/02-unit-tests.md`:

- `should_overlap_writes_when_concurrency_above_one` — a writer whose INSERT sleeps; assert total
  elapsed is nearer `total/N` than `total`.
- `should_surface_error_from_an_earlier_chunk` — fail chunk 2 of 6, assert the error reaches the
  engine within the following N `write()` calls.
- `should_fail_the_step_when_the_final_chunk_fails` — fail only the last chunk, assert the step
  ends in error. This is the test that validates the engine fix; without it the failure is
  invisible.
- `should_take_the_sequential_path_when_concurrency_is_one` — assert behaviour is identical to the
  current implementation.
- `should_warn_and_stay_sequential_on_sqlite` — assert the SQLite builder ignores the setting.

Integration, with testcontainers: a concurrent write of 10k rows lands exactly 10k rows, no
duplicates and no losses.

Measurement: benchmark at 1M rows with concurrency 1, 2, 4, 8. Record throughput and
`write_duration` per setting to find where the server saturates. The gain will not be linear in N
— PostgreSQL's WAL and primary-key maintenance become the limit, not the client.

## Semver

The engine's `close()` change is a behavioural break: target **0.5.0**. `StepExecution` is already
`#[non_exhaustive]`, so any new metric this work needs can be added without further breakage.
