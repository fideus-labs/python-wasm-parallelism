---
type: report
title: Node.js Benchmarks
created: 2026-07-17
tags:
  - benchmark
  - node
  - pyodide
related:
  - '[[phase-02-results]]'
  - '[[browser-benchmarks]]'
---

# Node.js Benchmarks

Serial-vs-parallel performance of the Pyodide worker pool in Node,
measured by `npm run bench` (bench/run-bench.ts). This file is
regenerated from `bench/results/node-2026-07-17.json` (`npm run bench:report`) — edit
the harness or the generator, not this file. The pool and async dask
scheduler under test are the Phase 01–02 builds summarized in
[[phase-02-results]]; the browser counterpart of this report is
[[browser-benchmarks]] (Phase 04).

## Environment

| | |
| --- | --- |
| Date | 2026-07-17 |
| Node | v24.18.0 |
| Pyodide | 314.0.2 (npm) / 314.0.2 (runtime) |
| OS | linux 6.6.114.1-microsoft-standard-WSL2 (arm64) |
| CPU | Qualcomm ARM64 (part 0x002), 16 logical cores (availableParallelism 16) |
| Pool sizes | 1 (serial), 2, 4, 8 |
| Repetitions | 3 timed (median reported) after 1 untimed warmup run per cell |

## Workloads

Every cell runs the same fixed total work; pools are warmed (interpreters
booted, packages installed/mirrored) before timing, and each cell runs one
additional untimed warmup repetition. Efficiency = speedup ÷ workers.

### Prime counting (pure Python, CPU-bound)

Trial-division prime counting from Phase 01; equal ranges dispatched with `pool.map()`. Fixed total work: primes in [2, 2000000) across 8 chunks.

| Workers | Median wall-clock | Speedup | Efficiency |
| --- | --- | --- | --- |
| serial (1) | 5.86 s | 1.00× | 100% |
| 2 | 3.27 s | 1.79× | 89% |
| 4 | 1.96 s | 2.98× | 75% |
| 8 | 1.23 s | 4.76× | 59% |

### Monte Carlo π estimation (random-heavy pure Python)

Seeded `random.Random` sampling; each chunk counts in-circle hits, dispatched with `pool.map()`. Fixed total work: 16,000,000 samples across 8 chunks.

| Workers | Median wall-clock | Speedup | Efficiency |
| --- | --- | --- | --- |
| serial (1) | 2.68 s | 1.00× | 100% |
| 2 | 1.43 s | 1.87× | 93% |
| 4 | 792 ms | 3.38× | 84% |
| 8 | 416 ms | 6.43× | 80% |

### numpy batch matmul (mirrored packages + serialization)

Driver Python submits cloudpickled numpy tasks via `pyodide_pool.submit`; workers mirror numpy, chain matmuls, and return the full float64 product matrix through cloudpickle. Fixed total work: 8 tasks × 20 chained matmuls of 256×256.

| Workers | Median wall-clock | Speedup | Efficiency |
| --- | --- | --- | --- |
| serial (1) | 1.82 s | 1.00× | 100% |
| 2 | 1.03 s | 1.76× | 88% |
| 4 | 662 ms | 2.75× | 69% |
| 8 | 440 ms | 4.13× | 52% |

### dask.delayed reduction graph (Phase 02 scheduler)

`dask.delayed` prime-count leaves feeding `sum()`, executed by the async scheduler via `pyodide_pool.compute(..., pool=...)`. Fixed total work: primes in [2, 2000000) across 8 delayed leaves.

| Workers | Median wall-clock | Speedup | Efficiency |
| --- | --- | --- | --- |
| serial (1) | 6.09 s | 1.00× | 100% |
| 2 | 3.36 s | 1.82× | 91% |
| 4 | 1.96 s | 3.11× | 78% |
| 8 | 1.27 s | 4.81× | 60% |

## Overheads

| Overhead | Median | Samples |
| --- | --- | --- |
| Per-worker Pyodide boot | 1.11 s | 15 cold boots |
| No-op task round-trip (warm worker) | 0.7 ms | 10 |
| 1 MiB numpy cloudpickle round-trip | 10.6 ms | 5 |

Per-cell warmup durations (first-touch package installs and mirroring
replays) are recorded in the results JSON under `cells[].warmupMs` and
`setup[]`.

## Analysis

- **Prime counting (pure Python, CPU-bound)** reaches 4.76× on 8 workers (59% efficiency). Scaling holds through the largest pool measured.
- **Monte Carlo π estimation (random-heavy pure Python)** reaches 6.43× on 8 workers (80% efficiency). Scaling holds through the largest pool measured.
- **numpy batch matmul (mirrored packages + serialization)** reaches 4.13× on 8 workers (52% efficiency). Scaling holds through the largest pool measured.
- **dask.delayed reduction graph (Phase 02 scheduler)** reaches 4.81× on 8 workers (60% efficiency). Scaling holds through the largest pool measured.

Boot amortization: each worker pays a one-time Pyodide boot of ~1.11 s
(median across 15 cold boots). Warmup boots interpreters in parallel, so a
pool is ready in roughly one boot's wall-clock, and recycled workers keep
their interpreter (`bootMs` = 0 on reuse). All timings above exclude boot;
a one-shot script whose total Python work is comparable to a single boot
cannot amortize the pool.

Serialization and dispatch: a no-op task round-trips in ~0.7 ms, and a
1 MiB float64 cloudpickle payload echo takes ~10.6 ms
(≈ 9.9 ms per MiB over the no-op floor, covering pickle + structured-clone
transfer + unpickle in both directions). Tasks should stay coarse relative
to these floors — the dask.bag finding from [[phase-02-results]] (tasks of
~90 ms barely profit) is the same effect.

Core count: the machine exposes 16 logical cores (availableParallelism
16); the matrix caps at 8 workers, each a single-threaded
WebAssembly interpreter, so ideal speedup equals the worker count as long
as the chunk count divides evenly across workers — the taper points above
mark where per-task overhead and shared-machine effects win instead.
