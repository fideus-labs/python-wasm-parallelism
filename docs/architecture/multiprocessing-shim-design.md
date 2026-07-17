---
type: analysis
title: multiprocessing.Pool Shim on the Pyodide Worker Pool — Design
created: 2026-07-17
tags:
  - multiprocessing
  - architecture
related:
  - '[[multiprocessing-on-wasm]]'
  - '[[dask-scheduler-design]]'
  - '[[pyodide-parallelism]]'
  - '[[worker-pool-api]]'
---

# `multiprocessing.Pool` Shim on the Pyodide Worker Pool — Design

Phase 06 adds the second backend over the Phase 01–02 machinery: a
`multiprocessing.Pool`-compatible API so existing Python code written
against the stdlib runs in Pyodide after changing one import line. The
research grounding is [[multiprocessing-on-wasm]] (why the stdlib fails,
what can honestly be emulated) and the substrate is the driver package from
[[dask-scheduler-design]] — `pyodide_pool._bridge` is the single choke
point for cloudpickle + JS bridging, and this shim adds **no second copy**
of that machinery.

The user-visible contract:

```python
# was: from multiprocessing import Pool
from wasm_multiprocessing import Pool

with Pool(4) as pool:
    results = await pool.amap(count_primes, ranges)      # portable everywhere
    results = pool.map(count_primes, ranges)             # only where blocking is legal (JSPI)
```

## Package shape: `python/wasm_multiprocessing/`

A second pure-Python wheel beside `python/pyodide_pool/`, with its own
`pyproject.toml`, depending on `pyodide_pool`:

```
python/wasm_multiprocessing/
├── pyproject.toml
└── wasm_multiprocessing/
    ├── __init__.py     # Pool, AsyncResult, cpu_count, TimeoutError; guarded stubs
    └── pool.py         # the shim itself
```

- **All remote execution goes through `pyodide_pool`**: single calls via
  `_bridge.submit` semantics, batched chunks via a small `_bridge.WorkerPool`
  extension over the JS pool's `mapPickled` (below). The shim never touches
  cloudpickle/`Uint8Array`/`JsException` itself — exception re-raising with
  `RemoteTraceback` chaining comes for free from `_raise_remote`.
- Like `pyodide_pool`, the package runs **only in the driver**; helpers that
  end up inside task payloads (the chunk runner) travel by value
  (`cloudpickle.register_pickle_by_value`), so workers never install it.
- `Pool()` binds to the default `pyodide_pool.WorkerPool` (registered JS pool
  or `loader.create_pool` in JupyterLite). The JS pool's `poolSize` is the
  **hard** concurrency cap; `Pool(processes=N)` applies a driver-side
  `asyncio.Semaphore(N)` as a **soft** cap on in-flight chunks, which is what
  makes pool-size 1/2/4 benchmarking meaningful on a single 4-worker JS pool.
  `processes=None` defaults to `wasm_multiprocessing.cpu_count()` =
  `js.navigator.hardwareConcurrency` (the `navigator` global exists in
  browsers, workers, and Node ≥ 21), falling back to 1.

### One `_bridge` extension: batch submit over `mapPickled`

`_bridge.WorkerPool` today exposes only single-call `submit`. The shim needs
one addition (in `pyodide_pool`, not duplicated in the shim):

```python
async def submit_batch(self, calls: Sequence[tuple[func, args, kwargs]]) -> list[Any]
```

cloudpickle each triple, hand the buffers to JS `mapPickled` (one worker-pool
run: FIFO queue, per-chunk progress, `cancel(runId)`, input-order results),
await the run promise, unpickle in order; a failed chunk re-raises the
original remote exception via the existing `_raise_remote`. `mapPickled`
rejects with the **first** failing chunk once the run settles — matching
stdlib `Pool.map`, which also surfaces one exception.

## API mapping

Legend: **supported** = stdlib-shaped, works everywhere · **sync-where-capable**
= works unchanged under JSPI, otherwise raises `RuntimeError` naming the exact
async replacement (see blocking strategy) · **async-only** = only an `a*` /
`async for` form exists · **unsupported** = raises immediately with a clear
error.

| `multiprocessing.Pool` member | Status | Shim form |
|---|---|---|
| `Pool(processes=None)` | supported | soft cap over the shared JS pool; default `cpu_count()` |
| `Pool(initializer=, initargs=)` | unsupported (v1) | `NotImplementedError`; planned as a cloudpickled per-worker prelude replayed in the chunk runner |
| `Pool(maxtasksperchild=, context=)` | unsupported | `NotImplementedError` (worker recycling is the JS pool's policy) |
| `map(func, it, chunksize=None)` | sync-where-capable | `run_sync(self.amap(...))`; error names `await pool.amap(...)` |
| `map_async(func, it, chunksize, callback, error_callback)` | supported | returns `AsyncResult` wrapping an asyncio task over `amap` |
| `starmap` / `starmap_async` | sync-where-capable / supported | over `astarmap`; error names `await pool.astarmap(...)` |
| `apply(func, args, kwds)` | sync-where-capable | over `aapply`; error names `await pool.aapply(...)` |
| `apply_async(func, args, kwds, callback, error_callback)` | supported | `AsyncResult`; callbacks fire from the task's done-callback |
| `imap(func, it, chunksize=1)` | async-only | **async generator**: `async for r in pool.imap(...)` — yields in input order as chunks complete |
| `imap_unordered(func, it, chunksize=1)` | async-only | async generator, completion order (`asyncio.as_completed` over per-chunk futures) |
| `close()` | supported | flips the pool to closed; later submissions raise `ValueError` (stdlib behavior) |
| `join()` | sync-where-capable | over `ajoin()` (await all outstanding results); stdlib rule kept: `ValueError` unless closed/terminated first |
| `terminate()` | supported (approximate) | cancel queued-not-started chunks (`cancel(runId)`), then JS `pool.terminate()`; **in-flight chunks run to completion** — no preemption without an interrupt buffer (future work) |
| `__enter__` / `__exit__` | supported | `__exit__` calls `terminate()` like stdlib; `async with` also provided |
| `AsyncResult.get(timeout=None)` | sync-where-capable | error names `await result.aget()`; `await result` is sugar for `aget()` |
| `AsyncResult.aget(timeout=None)` | supported (shim addition) | coroutine; timeout raises `wasm_multiprocessing.TimeoutError` (= `multiprocessing.TimeoutError`, importable on Emscripten) |
| `AsyncResult.wait(timeout=None)` | sync-where-capable | over `await_ready(timeout)` coroutine (returns `None`, never raises on timeout — stdlib behavior) |
| `AsyncResult.ready()` / `successful()` | supported | non-blocking; `successful()` raises `ValueError` before completion (stdlib behavior) |
| `cpu_count()` | supported | module level, `navigator.hardwareConcurrency` |
| `Process`, `Queue`, `Pipe`, `Manager`, `Value`, `Array`, `Lock`, … | unsupported | module `__getattr__` raises `NotImplementedError` pointing at this doc (see out of scope) |

Failure semantics mirror the stdlib + [[dask-scheduler-design]] error path:
a raising task re-raises the **original exception type** on `get()`/`amap`,
with the worker traceback chained as `RemoteTraceback`; `successful()`
returns `False`.

### Chunking semantics (`chunksize`)

- One **chunk = one task message**: the payload is
  `cloudpickle.dumps((_run_chunk, (func, items), {}))` where `_run_chunk`
  returns `[func(x) for x in items]` (or `func(*x)` for starmap). A
  `map`/`amap` call is **one `mapPickled` run** over its chunk payloads —
  order-preserving by construction, one progress tick and one cancel handle
  per call. The driver flattens chunk results back to item order.
- Default `chunksize=None` uses CPython's own `Pool._map_async` heuristic:
  `chunksize, extra = divmod(len(iterable), processes * 4)`; `+1` if `extra`
  — i.e. ≈ 4 chunks per (soft-cap) worker, so per-message overhead
  (pickle + postMessage + package-snapshot check, milliseconds per
  [[dask-scheduler-design]]) amortizes over coarse batches.
- `imap`/`imap_unordered` default `chunksize=1` (stdlib default) and dispatch
  **per-chunk `submit` futures** instead of one `mapPickled` run, because the
  run promise settles only as a whole: `imap` awaits chunk futures in
  submission order and yields items; `imap_unordered` yields whole chunks as
  they complete. Iterables are consumed eagerly at call time (v1); the
  semaphore still bounds in-flight chunks.
- `apply`/`aapply` is a single unbatched `submit`.

## Blocking strategy — what the spike actually showed

The sync-vs-async question was settled empirically with three re-runnable
spike scripts in `docs/architecture/spikes/` (`spike-node-atomics.mjs`,
`spike-node-jspi.mjs`, `spike-browser-worker.mjs` + its `…worker2.mjs`
refinement), run on Node v24.18.0 and Playwright Chromium 149 with COOP/COEP
headers.

**Spike 1 — Node main thread (the driver thread of `demo:*` and vitest):**

| Question | Result |
|---|---|
| `Atomics.wait` legal on Node main thread? | ✅ (`timed-out` after 50 ms, no throw) |
| worker_threads worker writes SAB + `Atomics.notify` while driver blocked? | ✅ woke in 102 ms with the written value |
| worker `postMessage` delivered while driver blocked? | ❌ `null` during a 1 s block; delivered only after unblock |
| queued microtasks run while driver blocked? | ❌ starved until unblock |

**Spike 2 — JSPI `run_sync` over the real pool** (driver Pyodide +
`dist/pyodide-worker.js` + `python/pyodide_pool`, the exact
`tests/helpers.ts` topology, entered via `runPythonAsync`):

- `node --experimental-wasm-jspi`: `pyodide.ffi.can_run_sync()` → `True`;
  `run_sync(pyodide_pool.submit(lambda a, b: a * b, 6, 7))` → `42` (1.3 s
  incl. worker boot); `run_sync(asyncio.gather(*8 submits))` → all 8 correct
  across 2 workers (1.1 s). **A blocking-looking map over the unmodified
  async pool works.**
- Without the flag: `can_run_sync()` → `False`, `run_sync` raises
  `RuntimeError: WebAssembly stack switching not supported in this
  JavaScript runtime`. Detection is clean and per-call.

**Spike 3 — browser, JupyterLite-kernel topology** (page → kernel-shaped
worker → **nested** pool worker, exactly how `loader.create_pool` wires
JupyterLite; Chromium 149, cross-origin isolated):

| Question | Result |
|---|---|
| `Atomics.wait` on the browser **main** thread | ❌ throws `TypeError` (confirmed) |
| `Atomics.wait` inside the kernel worker | ✅ legal |
| **warm** (fully booted) nested worker writes SAB + notify while kernel blocked | ✅ woke in 101 ms with the value |
| nested worker `postMessage` while kernel blocked | ❌ starved, exactly like Node |
| **cold** nested worker booted while parent is blocked | ❌ never boots — nested-worker script loading needs the parent's event loop; a 3 s wait timed out |
| `WebAssembly.Suspending` (JSPI) | `function` on the page **and** inside workers |

### Conclusions per environment

1. **Being allowed to block is not enough.** In every environment where
   `Atomics.wait` is legal (Node main thread, any worker), blocking the
   driver also freezes the JS event loop that the pool itself lives on:
   task dispatch runs through microtask chains and results arrive as
   `message` events, and the spikes show both are starved. A sync facade
   over the **existing** protocol deadlocks by construction — this is the
   [[dask-scheduler-design]] "why async" argument, now demonstrated, not
   argued.
2. **An Atomics path exists but is not free.** The mechanics are validated
   (spikes 1B, 3-C1): a pool worker *can* write result bytes into a
   `SharedArrayBuffer` mailbox and `Atomics.notify` a blocked driver, in
   Node and in the nested-worker browser topology. Turning that into a
   feature requires three things the current stack does not have:
   a worker-protocol extension (results written into a SAB, not
   `postMessage`d), a **synchronous dispatch path** (today the
   `postMessage` happens inside `@fideus-labs/worker-pool`'s async
   plumbing, which never runs once the driver blocks — spike 1D), and
   **pre-warmed workers** in the browser (spike 3-C2: a cold nested worker
   cannot boot under a blocked parent). That is a re-plumbing of
   worker-pool internals, not a shim-level feature.
3. **JSPI is the sync path that actually works today**, end-to-end, over
   the unmodified pool (spike 2), because `run_sync` suspends the WASM
   stack and lets the same event loop keep delivering pool messages. It is
   capability-detectable per call (`pyodide.ffi.can_run_sync()`).

### Committed strategy (v1)

Sync `map`/`starmap`/`apply`/`join`/`AsyncResult.get`/`AsyncResult.wait`
are **capability-detected, not environment-detected** — one rule everywhere:

```python
def _block_on(awaitable, replacement: str):
    try:
        from pyodide.ffi import can_run_sync, run_sync
        capable = can_run_sync()
    except ImportError:          # non-Pyodide Python (CI helpers, docs builds)
        capable = False
    if capable:
        return run_sync(awaitable)
    awaitable.close()            # don't leak a never-awaited coroutine
    raise RuntimeError(
        f"...blocking is not available here; use: {replacement}"
    )
```

- **Detection is per call**, not import-time: `can_run_sync()` depends on
  how Python was entered (async entry via `runPythonAsync` /
  `callPromising` allows stack switching; plain `runPython` does not).
- The `RuntimeError` message **must contain the exact async replacement
  call** — `await pool.amap(func, iterable)`, `await pool.astarmap(...)`,
  `await pool.aapply(...)`, `await pool.ajoin()`, `await result.aget()` —
  plus where JSPI is available (Chrome 137+ by default; Node 24 with
  `--experimental-wasm-jspi`; Firefox behind a flag; Safari not yet).
  The Phase 06 tests assert on the replacement substring.

Per environment this means:

| Environment | Sync `map` behavior (committed) |
|---|---|
| Browser main thread (page-embedded driver) | JSPI `run_sync` when `can_run_sync()` (Chrome 137+ default); otherwise the guidance `RuntimeError`. Never Atomics (illegal there anyway). |
| JupyterLite kernel worker (nested pool workers) | Same detection. JSPI is present in Chromium workers (spike 3) and the Pyodide kernel enters user code asynchronously, so `run_sync` is *expected* to work — to be verified live in this phase's JupyterLite task; the capability gate makes the fallback safe if it does not. Firefox/Safari kernels get the guidance error. |
| Node main thread (vitest, `demo:*`) | Same detection. **Default Node runs have no JSPI flag, so the committed, tested behavior is the guidance `RuntimeError`**; under `node --experimental-wasm-jspi` the sync methods genuinely block and return (validated by spike 2, optionally covered by a flag-gated test). |
| Node/browser worker as driver (off-main) | Same detection; `Atomics.wait` being legal there does not help the unmodified protocol (conclusion 1). |

The async-native core (`amap`, `astarmap`, `aapply`, `imap`,
`imap_unordered`, `aget`, `ajoin`) is the portable surface and the only
thing the other layers wrap — identical to the [[dask-scheduler-design]]
async-first decision.

**Future (explicitly deferred): SAB mailbox sync path** — worker writes
`[status, length, bytes...]` into a per-call SAB, driver `Atomics.wait`s;
requires the protocol/dispatch/warmup changes in conclusion 2 and a
result-size negotiation (grow-and-retry or two-phase length handshake).
Recorded so the door stays open; not part of the shim.

## Out of scope (v1, intentional)

- **`Process`** — no processes/threads to expose ([[multiprocessing-on-wasm]]);
  one-shot needs are `Pool(1).apply_async`.
- **Shared state: `Value`, `Array`, `shared_memory`, `Manager`** — workers
  share nothing; SAB-backed emulation is conceivable but out of scope.
- **`Queue` / `Pipe` / `Lock` / semaphores between user tasks** — no
  driver-bypassing channels exist between workers; tasks communicate only
  through arguments and results.
- **Fork semantics** — workers boot fresh; no copy-on-write globals. State
  ships explicitly in the pickled closure (and later via `initializer`
  replay). Documented loudly in the README/notebook.
- **`maxtasksperchild`, custom contexts** (`fork`/`spawn`/`forkserver`) —
  meaningless here; worker recycling is the JS pool's LIFO policy.

All of these raise `NotImplementedError` naming this document rather than
failing obscurely mid-task.

## Risks

- **JSPI stability**: pyodide#6106 reports crashes mixing JSPI with heavy
  asyncio on some machines; treated as promising-but-not-boring. The shim
  never *requires* JSPI — worst case users stay on the `a*` methods.
- **`can_run_sync()` variance across entry points**: notebook cells,
  `runPythonAsync`, and JS-called Python functions differ; per-call
  detection plus precise error text keeps failures actionable.
- **`imap` eager consumption**: v1 materializes the input iterable and
  dispatches all chunks up front (bounded in flight by the semaphore);
  stdlib streams lazily from a generator. Acceptable for the target
  workloads; noted for a later lazy-windowing pass.
