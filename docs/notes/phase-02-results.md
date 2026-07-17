---
type: note
title: Phase 02 Results — Dask Async Scheduler on the Pyodide Worker Pool
created: 2026-07-17
tags:
  - dask
  - results
related:
  - '[[dask-scheduler-design]]'
  - '[[phase-01-results]]'
---

# Phase 02 Results — Dask Async Scheduler on the Pyodide Worker Pool

End-to-end verification of the Phase 02 headline feature: dask task graphs
built in a driver Pyodide instance execute in parallel on the Phase 01 worker
pool via `await pyodide_pool.compute(...)`, with automatic package mirroring.
Architecture in [[dask-scheduler-design]]; the Phase 01 baseline the pool
carries over is in [[phase-01-results]].

## Verified outcome

`npm run demo:dask` (esbuild bundle + `tsx examples/node-dask-demo.ts`) exits 0
with all three demos producing results identical to dask's synchronous
scheduler. Two consecutive clean runs measured delayed-graph speedups of
**3.10x** and **3.06x** on 4 workers — comfortably above the 1.5x acceptance
threshold, and in line with the 3.17–3.21x the raw pool reached in Phase 01
(the scheduler layer costs almost nothing at this task granularity).

## Measured timings (two runs, 2026-07-17)

Setup (once per process):

| Step | Run 1 | Run 2 |
| --- | --- | --- |
| Worker pool boot (4 interpreters, parallel) | 1.26 s | 1.26 s |
| Driver Pyodide boot | 975 ms | 944 ms |
| Driver packages (cloudpickle, dask via micropip, `pyodide_pool` via FS) | 803 ms | 828 ms |
| Package-mirroring warm-up (dask → all 4 workers) | 1.09 s | 1.21 s |

**Demo 1 — `dask.delayed`:** primes in `[2, 2_000_000)` counted by
pure-Python trial division in 8 chunks feeding a `sum()` reduction
(total = 148 933 = π(2·10⁶) ✓ both paths, both runs):

| Scheduler | Run 1 | Run 2 |
| --- | --- | --- |
| dask synchronous | 5.81 s | 5.85 s |
| `pyodide_pool.compute` (4 workers) | 1.87 s | 1.91 s |
| **Speedup** | **3.10x** | **3.06x** |

**Demo 2 — `dask.bag`:** `from_sequence(range(24), npartitions=4)
.map(busy_square).sum()` = 13 797 447 on both paths; 358/348 ms sync vs
280/262 ms pooled. The modest gain is expected: partitions are ~90 ms of
work each, so per-task overhead (cloudpickle + postMessage + snapshot replay
check, single-digit ms) and the 4-deep reduction tree eat most of the
parallel headroom. This matches design-doc risk #1 — coarse tasks are the
target workload.

**Demo 3 — numpy package mirroring:** see next section.

Environment: Node v24.18.0, `pyodide` npm 314.0.2 (CPython 3.14.2), dask
2026.7.1 + cloudpickle 3.1.2 (both from PyPI/distribution at run time),
16 logical CPUs, Linux (WSL2).

## Package-mirroring behavior observed

- **First task pays, later tasks ride free.** With numpy loaded only in the
  driver, a batch of 4 numpy-using `delayed` tasks (one per worker) completed
  in **426/404 ms** while every worker auto-installed numpy from the mirrored
  snapshot; an identical second batch took **42/40 ms** — a ~10x drop, pure
  compute + transport once the worker-side installed-set short-circuits the
  replay. Results matched driver-local numpy exactly (module refs pickle by
  reference, so correct results *prove* the worker-side install happened
  before unpickling).
- **Dask itself mirrors the same way.** The demo warms all 4 workers with
  concurrent trivial submits right after driver setup, so the one-time
  dask-to-workers mirror cost is visible as its own line (~1.1–1.2 s for all
  4 workers in parallel) instead of polluting Demo 1's numbers. Workers need
  dask because modern task-spec graph nodes pickle by reference to it
  (deviation #3 below).
- **Snapshot replay is idempotent and cheap.** A fresh snapshot rides on
  every `execPickled` message (verified by a monkeypatch counter in
  `tests/pyodide-pool-python.test.ts`); replaying it against an
  already-converged worker costs only set lookups — the 42/40 ms second
  batch above is the end-to-end proof. No separate sync machinery was needed,
  as the design doc predicted.
- Worker-side micropip progress ("Loading numpy…") surfaces on the console
  during first installs — routed through `console.log` by the Phase 02 side
  fix (Node `worker_threads` have no `process.stdout.fd`, so Pyodide's
  default stdout device would throw).

## Deviations from the design doc

Found while implementing against real dask 2026.7.1 (probe script kept at
the playbook's `Working/probe-dask.mjs`); the design doc has been updated to
match the implementation where it described the older APIs:

1. **dask is not in the Pyodide v314 distribution.** The design doc had this
   right all along (the Phase 02 task text's "it is in the Pyodide
   distribution" was wrong); the driver installs dask with
   `micropip.install("dask")` from PyPI (~0.8 s including cloudpickle and
   distribution deps).
2. **`dask.base.collections_to_dsk` and `dask.core.nested_get` no longer
   exist.** `compute` now follows modern expression-based
   `dask.base.compute`: `collections_to_expr` + `FinalizeCompute` +
   `expr.optimize()`, with each collection's finalization compiled into the
   graph, flat keys, and `repack(results)` reassembly. The classic
   `collections_to_dsk` + `__dask_postcompute__` path survives as an
   `ImportError` fallback for older dask; nested-key assembly is a local
   `_nested_get` helper.
3. **Modern task-spec nodes need `GraphNode`-aware dispatch.**
   `dask.delayed`/`dask.bag` emit `dask._task_spec` nodes, and
   `istask(Alias)` is `True` — so classification checks `GraphNode` *before*
   `istask`: `Alias`/`DataNode` resolve locally (no worker round-trip), real
   `Task` nodes go remote as callables over a dependency-value mapping and
   pickle by reference to dask (hence mirroring, above). Legacy tuple tasks
   still ship with the by-value `_evaluate_node` evaluator, keeping workers
   dask-free for that dialect.
4. Minor: cloudpickle in this Pyodide build is 3.1.2 (doc said 3.1.1), and
   the scheduler grew a `pool=` kwarg for explicit `WorkerPool` wiring in
   tests; `compute` returns a bare result for a single argument (matching
   `obj.compute()`) and a tuple for several.

## Implications for later phases

- Scheduler overhead is negligible at ≥ hundreds-of-ms task granularity
  (Demo 1) and visible but tolerable at ~90 ms granularity (Demo 2) — the
  Phase 03 benchmarks should sweep task size to chart the crossover.
- Mirroring makes "driver has it → workers have it" invisible to users, but
  first-touch installs (dask ~1.1 s/pool, numpy ~0.4 s/pool) argue for the
  demo's pattern of warming workers immediately after pool creation; worth
  wrapping as a `pyodide_pool.warm()` convenience if Phase 05/06 users hit it.
- Everything above ran in Node; the browser path (Phase 04) re-times boot and
  mirroring against CDN-served wheels instead of `node_modules`/local disk.
