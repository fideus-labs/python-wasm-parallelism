---
type: analysis
title: Dask Scheduler vs multiprocessing Shim — Comparison and Recommendation
created: 2026-07-17
tags:
  - comparison
  - dask
  - multiprocessing
related:
  - '[[dask-scheduler-design]]'
  - '[[multiprocessing-shim-design]]'
  - '[[node-benchmarks]]'
  - '[[browser-benchmarks]]'
---

# Dask Scheduler vs `multiprocessing` Shim — Comparison and Recommendation

The project ships two Python-facing frontends over one substrate. Both run
on the same JS `PyodidePool` (one Pyodide interpreter per Web Worker /
`worker_thread`, LIFO recycling), both funnel every remote call through
`pyodide_pool._bridge` (cloudpickle wire format, package-snapshot
mirroring, original-exception re-raising with `RemoteTraceback` chained as
`__cause__`), and both are driver-only packages whose worker-side helpers
travel by value inside the pickled payload. The designs are
[[dask-scheduler-design]] (Phase 02) and [[multiprocessing-shim-design]]
(Phase 06); the measurements cited below are [[node-benchmarks]],
[[browser-benchmarks]], and the in-notebook head-to-head added to
`demos/jupyterlite/files/03-benchmark.ipynb` in Phase 06.

Because the substrate is shared, the differences below are real API and
scheduling differences, not implementation accidents — and the two
frontends compose: one session can drive both against the same JS pool.

## At a glance

| | dask backend (`pyodide_pool`) | `multiprocessing` shim (`wasm_multiprocessing`) |
|---|---|---|
| Mental model | declarative task **graph**, computed at once | imperative **flat map** over an iterable |
| Entry point | `await pyodide_pool.compute(obj)` | `Pool(N)` + `amap`/`map`/`apply_async`/… |
| Dependencies between tasks | yes — arbitrary DAGs, dispatch-when-ready | no — chunks of one call are independent |
| Sync (blocking) calls | none — async-only surface | capability-detected: real blocking under JSPI, guidance `RuntimeError` otherwise |
| Porting existing code | code already using dask: swap `.compute()` | code using stdlib `multiprocessing`: swap the import |
| Worker-side installs | dask mirrored to workers for modern graphs (~1.1 s first touch per 4-worker pool) | none — chunk runner travels by value |
| Driver-side installs | dask via micropip (~0.8 s) | none beyond `pyodide_pool` itself |
| Batching | user's graph partitioning is the batching | `chunksize` (CPython's own ≈ 4-chunks-per-worker heuristic) |
| Measured overhead vs raw pool (same workload, 4 workers) | ≈ 0% coarse tasks; +21% at ~95 ms tasks | −6% at ~95 ms tasks (batching wins back the floor) |

## API ergonomics

**The dask backend is dask.** Users build graphs with `dask.delayed`,
`dask.bag`, or dask arrays exactly as they would anywhere else; the only
project-specific surface is the entry point, `await
pyodide_pool.compute(...)` / `await pyodide_pool.get(dsk, keys)`. Because
dask's `compute(scheduler=...)` protocol is synchronous with no async
variant, the scheduler cannot hide behind the standard `scheduler=` seam —
the `await` is the one visible deviation, and it is unavoidable (a blocking
scheduler deadlocks the browser main thread by construction; see
[[dask-scheduler-design]], "Why the scheduler must be async"). Results are
bit-identical to dask's synchronous scheduler, verified in every demo and
test.

**The shim is the stdlib, reshaped for the platform.** `Pool(processes=N)`,
`map`/`starmap`/`apply`, `map_async`/`apply_async` returning `AsyncResult`
(`get`, `wait`, `ready`, `successful`), `imap`/`imap_unordered`,
`close`/`join`/`terminate`, context-manager protocol, module-level
`cpu_count()`. Two platform adaptations:

- **Async-native twins are the portable surface**: `amap`, `astarmap`,
  `aapply`, `aget`, `ajoin`, and `imap`/`imap_unordered` as async
  generators. These work in every environment.
- **The stdlib sync methods are capability-detected per call**: where JSPI
  stack switching is available (`pyodide.ffi.can_run_sync()` — Chrome 137+
  by default, Node 24 with `--experimental-wasm-jspi`), `pool.map` et al.
  genuinely block and return, byte-for-byte stdlib-shaped. Everywhere else
  they raise a `RuntimeError` whose message contains the exact replacement
  call (`await pool.amap(func, iterable)`), so the failure is its own
  migration guide.

One ergonomic gotcha carried over deliberately from the stdlib: `with
Pool(...)` calls `terminate()` on exit, which kills the shared JS pool's
warm workers; the next use pays re-boot. The notebooks re-warm after
context-managed sections. dask's frontend has no equivalent trap — it never
manages pool lifecycle.

Error semantics are identical in both frontends (they share
`_raise_remote`): the original exception type re-raises on the driver with
the worker traceback attached as a `RemoteTraceback` cause — dask on
`compute`, the shim on `get()`/`amap`, with `successful() → False` besides.

## Porting cost

**Existing `multiprocessing` code → shim: one import line, plus `await` if
you leave JSPI.** The Phase 06 porting test and the
`04-multiprocessing.ipynb` demo run a classic count-primes script after
changing `from multiprocessing import Pool` to `from wasm_multiprocessing
import Pool` and awaiting the entry point. Under Chromium's default JSPI
even the awaiting is unnecessary — the notebook's synchronous `pool.map`
cell blocks and returns for real. The honest caveats are the out-of-scope
list ([[multiprocessing-shim-design]]): no `Process`, no shared
`Value`/`Array`/`Manager`, no inter-task `Queue`/`Pipe`/`Lock`, no fork
semantics — workers boot fresh and state ships explicitly in the pickled
closure. Code that only uses `Pool` + results ports mechanically; code
that leans on shared state does not port at all (and fails with a
`NotImplementedError` naming the design doc, not obscurely mid-task).

**Existing dask code → dask backend: swap `obj.compute()` for `await
pyodide_pool.compute(obj)`.** Graph construction, collections, and
tokenization are untouched — dask itself is installed in the driver from
PyPI via micropip. Nothing else changes.

**Fresh code** written for this platform: a plain data-parallel loop is
less ceremony as `pool.amap(f, xs)` than as `compute([delayed(f)(x) for x
in xs])`; anything with inter-task structure is the dask backend's home
turf (next section).

## Scheduling capabilities: graphs vs flat maps

The dask backend executes **arbitrary DAGs**. The async Kahn-style
executor dispatches every ready task immediately, so independent branches
overlap and downstream tasks start the moment their inputs land —
fan-out/fan-in reductions, diamond dependencies, shared intermediates
computed once, multi-collection `compute(a, b)`. Cheap graph nodes
(`Alias`, `DataNode`) resolve locally without a worker round-trip; the
first failure cancels outstanding work and re-raises.

The shim executes **flat maps**. One `amap` call is one ordered
`mapPickled` run over chunk payloads (or semaphore-gated per-chunk submits
when `Pool(N)` soft-caps below the JS pool size); there is no notion of a
task depending on another task. Staged pipelines are expressed as
consecutive maps with a full barrier between stages — results of stage 1
return to the driver before stage 2 dispatches — where dask would stream
individual stage-2 tasks as their stage-1 inputs complete. `AsyncResult`
callbacks and `imap_unordered`'s completion-order streaming recover some
overlap, but coordination beyond that is the caller's job.

What the shim's narrower model buys is **batching**: `chunksize` (default:
CPython's own `divmod(len(iterable), processes * 4)` heuristic) packs many
items into one task message, amortizing the per-message floor. The dask
backend sends one message per graph task; its "batching" is whatever
partitioning the user built into the graph (`npartitions`, chunk shapes) —
idiomatic dask, but nothing recovers a too-fine graph automatically.

Both frontends are ultimately bounded by the same hard cap, the JS pool's
`poolSize`; `Pool(processes=N)` adds a driver-side soft cap, which is what
makes pool-size sweeps meaningful on a shared 4-worker pool.

## Overheads, as measured

The shared floors, from [[node-benchmarks]]: ~1.11 s per-worker boot in
Node (1.43 s in the browser from the CDN, [[browser-benchmarks]]),
~0.7 ms no-op task round-trip warm, ~10.6 ms per MiB of cloudpickle
payload round-trip. Both frontends sit on these identically.

**At coarse granularity the frontends are free.** The Node benchmark runs
the same fixed workload (primes below 2,000,000 in 8 chunks, ≈ 730 ms of
Python per chunk) as a raw `pool.map()` and as a `dask.delayed` graph with
a `sum` reduction: 1.96 s vs 1.96 s on 4 workers (2.98× vs 3.11× against
their own serial baselines; the graph's extra reduction node costs
nothing measurable). The shim's `04-multiprocessing.ipynb` timing cell
reaches 3.28× on `Pool(4)` vs `Pool(1)` in Chromium — the same story from
the third frontend.

**At ~100 ms granularity the frontends separate.** The Phase 06 section of
`03-benchmark.ipynb` runs one workload three ways on the same warmed
4-worker in-browser pool — primes below 1,000,000 in 8 chunks (≈ 95 ms of
Python each; total = 78,498 asserted identical on every path), single run,
Chromium, 2026-07-17:

| Path | Wall-clock | vs raw submits |
|---|---|---|
| 8 raw `pyodide_pool.submit` coroutines, gathered | 0.76 s | — |
| `dask.delayed` graph + sum, `pyodide_pool.compute` | 0.93 s | +21% |
| shim `Pool(4).astarmap(..., chunksize=1)` | 0.72 s | −6% |

The +21% is the dask layer itself — expression optimization, task-spec
dispatch, one message per graph node — and matches the Phase 02 dask.bag
observation (~90 ms partitions: 358 ms sync vs 280 ms pooled, most of the
parallel headroom eaten). The shim's −6% is the batch path being *leaner*
than gathered raw submits: one `mapPickled` run, one settle, ordered
reassembly in a list comprehension. At this granularity `chunksize > 1`
would widen the shim's lead further; the notebook deliberately uses
`chunksize=1` to measure the per-message case.

**Package mirroring is a structural difference.** Modern dask task-spec
graph nodes pickle by reference to `dask`, so workers must have dask
mirrored — a one-time ~1.1–1.2 s per 4-worker pool (Phase 02, measured),
on top of the driver's ~0.8 s micropip install. The shim's chunk runner
travels by value (`register_pickle_by_value`), so `wasm_multiprocessing`
adds **zero** worker-side installs — workers only ever install what user
task code itself imports (numpy etc., mirrored identically under both
frontends: ~0.4 s first touch, ~40 ms replay).

## Recommendation

**Port, don't rewrite.** Code already written against `multiprocessing.Pool`
gets the shim; code already written against dask gets the scheduler. Both
ports are one-line-shaped, and rewriting across the pair buys nothing —
the substrate and floors are identical.

**For new code, pick by the shape of the work:**

- **Flat data-parallelism** (map a function over items, gather results) →
  the **shim**. Stdlib ergonomics, `chunksize` batching that automatically
  amortizes fine-grained items, no dask install in driver or workers, and
  the only sync-blocking story on the platform (JSPI `pool.map`) when a
  plain script "just working" matters — the measured overhead vs raw pool
  access is negative.
- **Graph-shaped work** (dependencies, shared intermediates, fan-in
  reductions, staged pipelines) or **dask collections** (`bag`, arrays) →
  the **dask backend**. Dispatch-when-ready overlap across stages is
  something the shim structurally cannot express, and at the coarse task
  sizes this platform wants anyway (≥ hundreds of ms — see the floors
  above), the scheduler layer is free. Keep tasks coarse: at ~100 ms tasks
  the graph machinery costs a measured ~20%.
- **Mixed sessions are fine.** Both frontends share one JS pool and one
  bridge; a notebook can `await compute(...)` a graph, then `Pool(4).amap`
  a sweep, without a second pool or duplicated boot cost. The one
  interaction to respect: the shim's `with Pool(...)` **terminates** the
  shared pool's workers on exit (stdlib semantics) — prefer explicit
  `close()`/`ajoin()` or re-warm afterwards in long-lived sessions.

Either way, the async surface (`await compute`, `amap`, `aget`) is the
portable core to teach and document; the shim's synchronous methods are a
compatibility bridge that lights up where JSPI exists, not the foundation
([[multiprocessing-shim-design]], blocking strategy).
