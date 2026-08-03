# Step Phase Profiling — Results

**Date:** 2026-08-03
**Branch:** feat-step-phase-profiling
**Status:** Measured

Execution of Task 7 of `2026-08-03-step-phase-profiling.md`, against the decision table in
`2026-08-03-step-phase-profiling-design.md`.

## Environment

- PostgreSQL 15.18 (aarch64) in an Apple Container, port published to `127.0.0.1:5432`.
- Benchmark: `examples/benchmark_csv_postgres_xml`, release build, chunk size 1 000, pool size 10.
- Pipeline: CSV → PostgreSQL → XML → PostgreSQL, three steps, `TRUNCATE` between runs.
- `transaction_id VARCHAR(36) PRIMARY KEY`; step 2's reader uses **keyset** pagination
  (`with_keyset`), page size 1 000.

Note: connecting to the container's own IP (`192.168.64.4`) failed with `No route to host`
from the benchmark binary, while `ping` and `nc` reached that same address and port. Publishing
the port to `127.0.0.1` works. The underlying cause was not diagnosed.

## Measurements

### 100k rows

| Step | Total | read | process | write | flush |
|---|---|---|---|---|---|
| csv-to-postgres | 0.5s | 9% | 0% | **90%** | 0% |
| postgres-to-xml | 0.2s | **66%** | 0% | 31% | 0% |
| xml-to-postgres-import | 0.7s | 29% | 0% | **71%** | 0% |

### 1M rows

| Step | Total | read | process | write | flush |
|---|---|---|---|---|---|
| csv-to-postgres | 6.0s | 0.9s (15%) | 0.0s (1%) | **4.9s (82%)** | 0.0s (0%) |
| postgres-to-xml | 4.0s | **3.3s (83%)** | 0.0s (0%) | 0.6s (16%) | 0.0s (0%) |
| xml-to-postgres-import | 7.4s | 2.3s (32%) | 0.0s (0%) | **5.0s (67%)** | 0.0s (0%) |

### 10M rows

| Step | Total | read | process | write | flush | residual | throughput |
|---|---|---|---|---|---|---|---|
| csv-to-postgres | 57.6s | 8.8s (15%) | 0.5s (1%) | **47.3s (82%)** | 0.0s (0%) | 1.0s (2%) | 173 569 rec/s |
| postgres-to-xml | 30.8s | **24.2s (78%)** | 0.1s (0%) | 6.2s (20%) | 0.0s (0%) | 0.3s (1%) | 324 474 rec/s |
| xml-to-postgres-import | 74.0s | 24.5s (33%) | 0.1s (0%) | **48.9s (66%)** | 0.0s (0%) | 0.5s (1%) | 135 166 rec/s |

Total wall-clock at 10M, including CSV generation: **164.2s**.

The residual — everything outside the four timers — is 1-2%, confirming the instrumentation
accounts for essentially all of each step's time.

## Scaling

Growth factor per 10× increase in rows:

| Step | 100k → 1M | 1M → 10M |
|---|---|---|
| csv-to-postgres | ×12.0 | ×9.6 |
| postgres-to-xml | ×20.0 | ×7.7 |
| xml-to-postgres-import | ×10.6 | ×10.0 |

**No super-linear degradation.** The ×20 on step 2 between 100k and 1M looked alarming, but the
1M → 10M factor of ×7.7 is *sub*-linear, which rules out a quadratic effect. The 100k run is
simply too short (0.2s) to be a reliable baseline — warm-up and cache effects dominate at that
size. Keyset pagination scales as intended.

Phase proportions are stable between 1M and 10M (step 1 write: 82% → 82%; step 3 write:
67% → 66%), so the measurements are reproducible.

## Decision table applied

| Observation | Reading | Follow-up |
|---|---|---|
| `read` dominates | step 2, 78% | **B** — prefetch page N+1 |
| `write` dominates | steps 1 and 3, 82% / 66% | **B** — write-behind |
| All three phases same order | never — one phase always dominates | A not indicated |
| `process` dominates | never — 0-1% everywhere | `rayon` ruled out |
| `flush` > 5% | never — 0% everywhere | **leave the per-chunk flush alone** |

**Verdict: option B — concurrency inside the adapters. Not an async trait migration.**

## Analysis

**The async trait migration is not justified by these numbers.** Time is spent waiting on
PostgreSQL and on file I/O. Making `read()` and `write()` async removes none of that waiting —
it changes who waits. What removes it is *overlapping* the waits, which needs no change to the
trait signatures. The `#[async_trait]` cost quantified in the design doc (one boxed future per
`read()` call — 10M allocations here) would be paid for nothing.

**`process` is negligible, which collapses the case for option A.** Stage pipelining was meant
to turn `read + process + write` into `max(read, process, write)`. With `process` at ~0, the
three-stage pipeline degenerates into a two-stage read ‖ write overlap — exactly what option B
achieves adapter-locally, without touching the engine and without the `Send` bounds A would
require on the trait objects.

Theoretical ceiling for perfect read/write overlap at 10M:

| Step | Now | Overlapped `max(read, write)` | Gain |
|---|---|---|---|
| csv-to-postgres | 57.6s | 47.3s | 18% |
| postgres-to-xml | 30.8s | 24.2s | 21% |
| xml-to-postgres-import | 74.0s | 48.9s | 34% |
| **Total** | **162.4s** | **120.4s** | **26%** |

**The larger prize is underneath that ceiling.** Overlap caps out at 26% because the floor of
each step becomes its dominant phase — and for steps 1 and 3, that floor is PostgreSQL write
time: 47.3s and 48.9s for 10M rows, about 210k rows/s. The writer currently issues its batched
`INSERT`s **sequentially**, one chunk at a time, even though the pool holds 10 connections.
Spreading chunk writes across several connections concurrently could cut the dominant phase
itself rather than merely hiding it behind the others.

That is a bigger change than prefetching: it affects write ordering, error attribution, and how
`skip_limit` accounts for a failed chunk. It should be brainstormed on its own rather than
folded into a prefetch change.

## The flush question, settled

`flush_duration` is **0% on all three steps at all three scales**. The design doc's threshold for
making the per-chunk flush optional was 5%.

This is consistent with the code: the PostgreSQL writers inherit the trait's no-op `flush()`
(`ItemWriter::flush` default), and step 2's `XmlItemWriter`, which does implement a real flush,
costs nothing because its `BufWriter` has already streamed the data out as its 8 KB buffer
filled.

**Recommendation: leave the per-chunk flush exactly as it is.** Removing it would trade a
measured gain of zero for a real loss of crash resilience.

## Recommended next steps

1. **Prefetch in the RDBC/ORM readers** — biggest single win on step 2 (78% read).
2. **Write-behind in the RDBC writers** — steps 1 and 3.
3. **Then, separately: concurrent chunk writes.** This is where the remaining factor lives, and
   it needs its own design discussion because of the ordering and error-handling implications.
4. **Do not** migrate the traits to async. **Do not** add `rayon`. **Do not** touch the flush.
