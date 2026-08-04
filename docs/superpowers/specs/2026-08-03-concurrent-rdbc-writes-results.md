# Concurrent RDBC Writes — Measurement Results

Measurement plan: `docs/superpowers/plans/2026-08-03-concurrent-rdbc-writes.md`, Task 6 Step 4.

Measured 2026-08-04.

## Setup

- 1 000 000 rows, chunk = 1 000, 8 columns.
- PostgreSQL 15 in Apple `container` (4 vCPU, 1 GB), port published on `127.0.0.1:5432`.
- Connection pool: `max_connections(10)` — every setting below stays under that ceiling.
- `.with_concurrency()` applies to steps 1 and 3 only (the two PostgreSQL writers).
  Step 2 writes XML and is unaffected; it serves as a control.

```bash
DATABASE_URL="postgresql://postgres:postgres@127.0.0.1:5432/benchmark" \
  TOTAL_RECORDS=1000000 WRITE_CONCURRENCY=<N> \
  cargo run --release --example benchmark_csv_postgres_xml \
  --features csv,xml,rdbc-postgres 2>&1 | grep "Step '"
```

## Results

| WRITE_CONCURRENCY | step 1 write | step 3 write | step 1 total | step 3 total | wall-clock | throughput |
|---|---|---|---|---|---|---|
| 1 | 4.7 s | 5.0 s | 5.8 s | 7.4 s | 16.7 s | 59 934 rec/s |
| 2 | 1.2 s | 0.3 s | 2.0 s | 2.2 s | 7.3 s | 136 460 rec/s |
| 4 | 0.8 s | 0.1 s | 1.2 s | 2.0 s | 6.5 s | 154 776 rec/s |
| 8 | 0.7 s | 0.1 s | 1.1 s | 2.0 s | 6.2 s | 160 075 rec/s |

Control: step 2 (`postgres-to-xml`, no concurrency applied) stayed at 3.0–3.5 s across
all four settings, as expected.

**Cold-cache check.** `WRITE_CONCURRENCY=1` ran first, so it was re-run last, with the
cache warm: 17.2 s (vs 16.7 s). The sequential baseline is genuinely the slowest — the
gain is not a warm-up artefact.

**Row-count check.** After the `WRITE_CONCURRENCY=8` pass, both tables held exactly
1 000 000 rows. Concurrent writes lost and duplicated nothing at 1M rows, 1 000 chunks
deep.

## Reading of the numbers

- **The write phase collapses.** Step 1's write goes 4.7 s → 0.7 s (6.7×), step 3's
  5.0 s → 0.1 s (50×). This confirms the diagnosis from the phase-profiling study: the
  sequential writer left a 10-connection pool being used one connection at a time.
- **Gains are strongly sub-linear, and saturate at 4.** 1 → 2 halves the wall-clock;
  2 → 4 buys 0.8 s; 4 → 8 buys 0.3 s. Past 4, PostgreSQL's WAL and index maintenance
  are the limit, not the client.
- **Recommended ceiling: 4.** 8 is measurably but marginally faster (5 %) while doubling
  the delay on error reporting and the number of unordered in-flight chunks. Not worth it.
- **The bottleneck moves, it does not vanish.** At concurrency ≥ 2 both write steps
  become read-bound (step 3's read share goes 32 % → 92 %), and step 2 — untouched by
  this feature — becomes the single largest step. Further work belongs on the read side.

End-to-end: 16.7 s → 6.2 s, i.e. **2.7× on the full 3-step pipeline** for a 1-line
opt-in change.

## Still not verified

`tests/rdbc_postgres.rs::should_write_every_row_with_concurrency` compiles but has
never run: it uses testcontainers, which requires a Docker Engine API socket, and this
machine runs Apple `container` instead. The row-count check above covers the same claim
empirically, at 100× the scale, but the test itself remains unexecuted.
