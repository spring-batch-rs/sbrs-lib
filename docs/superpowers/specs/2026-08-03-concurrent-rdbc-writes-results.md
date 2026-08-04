# Concurrent RDBC Writes — Measurement Results

Measurement plan: `docs/superpowers/plans/2026-08-03-concurrent-rdbc-writes.md`, Task 6 Step 4.

## Status: NOT MEASURED — blocked

The benchmark was wired (`WRITE_CONCURRENCY` env var on the two PostgreSQL write
steps of `examples/benchmark_csv_postgres_xml.rs`) and compiles in release mode,
but the measurement itself has not been run.

Blocker, as of 2026-08-04:

- No Docker-compatible runtime available on this machine
  (`unix:///Users/sboussekeyt/.docker/run/docker.sock` — daemon not running).
- No PostgreSQL listening on `127.0.0.1:5432` either.

The same blocker prevented running the testcontainers integration test
`should_write_every_row_with_concurrency` in `tests/rdbc_postgres.rs`. That test
compiles but has never executed, so **the end-to-end claim that 10 000 rows land
exactly once under concurrency is unverified**.

## How to run it once a database is available

```bash
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=benchmark postgres:15
```

Then, for each of `WRITE_CONCURRENCY=1`, `2`, `4`, `8`:

```bash
DATABASE_URL="postgresql://postgres:postgres@127.0.0.1:5432/benchmark" \
  TOTAL_RECORDS=1000000 WRITE_CONCURRENCY=1 \
  cargo run --release --example benchmark_csv_postgres_xml \
  --features csv,xml,rdbc-postgres 2>&1 | grep "Step '"
```

And the integration test:

```bash
cargo test --all-features --test rdbc_postgres should_write_every_row_with_concurrency
```

## Table to fill in

One row per setting, taking `write_duration` from the step phase summary.

| WRITE_CONCURRENCY | step 1 write_duration | step 3 write_duration | total wall-clock |
|---|---|---|---|
| 1 | | | |
| 2 | | | |
| 4 | | | |
| 8 | | | |

Expect sub-linear gains: PostgreSQL's WAL and primary-key maintenance become the
limit. If 8 is no better than 4, that is the saturation point and should be
recorded as the recommended ceiling in
`sbrs-docsite/src/content/docs/reference/performance.mdx`.
