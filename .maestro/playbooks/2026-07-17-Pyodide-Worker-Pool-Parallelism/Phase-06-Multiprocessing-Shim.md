# Phase 06: Python `multiprocessing` Shim

This phase explores the second backend approach: a `multiprocessing`-compatible API (`Pool`, `map`, `apply_async`, `AsyncResult`, ...) implemented over the same worker-pool machinery, so existing Python code written against `multiprocessing.Pool` can run in Pyodide with minimal changes. It confronts the sync-vs-async question head-on: async variants everywhere, true synchronous blocking where the platform allows it, and clear errors where it does not. It closes with tests, a JupyterLite demo, and a comparison write-up of the dask and multiprocessing approaches.

## Tasks

- [x] Write the design doc `docs/architecture/multiprocessing-shim-design.md` (front matter `type: analysis`, tags `[multiprocessing, architecture]`, wiki-links `[[multiprocessing-on-wasm]]`, `[[dask-scheduler-design]]`) before implementing. Re-read `docs/research/multiprocessing-on-wasm.md` and the existing `python/pyodide_pool/_bridge.py` to maximize reuse. The doc must include:
  - An API mapping table: each `multiprocessing.Pool` member (`map`, `map_async`, `starmap`, `starmap_async`, `imap`, `imap_unordered`, `apply`, `apply_async`, `close`, `join`, `terminate`, context-manager protocol, plus `cpu_count()`) → supported | async-only | unsupported-with-clear-error, and the chosen chunking semantics for `chunksize`
  - The blocking strategy per environment: browser main thread (sync methods raise a helpful error pointing to the `a*` variants, unless JSPI `pyodide.ffi.run_sync` is detected at runtime), JupyterLite kernel worker and Node (investigate whether synchronous waiting is achievable in each — e.g. Atomics-based waiting where the caller is off the main thread — and document what actually works based on a small spike, not speculation)
  - What is intentionally out of scope: `Process`, shared `Value`/`Array`, `Manager`, `Queue` between user processes

  > **Done (2026-07-17).** Doc at `docs/architecture/multiprocessing-shim-design.md`; blocking strategy grounded in three re-runnable spikes committed to `docs/architecture/spikes/`. Spike findings: (1) wherever `Atomics.wait` is legal (Node main, workers) it starves the pool's own microtasks/`message` events, so a sync facade over the existing protocol deadlocks; a SAB-mailbox path is mechanically validated (worker wakes a blocked driver in ~100 ms, Node and browser-nested-worker alike) but needs a protocol extension + synchronous dispatch + pre-warmed workers (cold nested workers can't boot under a blocked parent), so it's deferred. (2) JSPI `run_sync` works END-TO-END over the unmodified pool under `node --experimental-wasm-jspi` (submit → 42; 8-task gather correct). Committed v1 strategy: sync methods are capability-detected via `pyodide.ffi.can_run_sync()` — `run_sync` when true, otherwise `RuntimeError` naming the exact `await pool.amap(...)`-style replacement; default (flagless) Node tests must assert the guidance error.

- [ ] Implement the shim as `python/wasm_multiprocessing/` (own `pyproject.toml`, pure-Python wheel, depends on `pyodide_pool` for `_bridge.submit` — do not duplicate the cloudpickle/JS bridging):
  - `pool.py` — `Pool(processes=None)` (defaults to `js.navigator.hardwareConcurrency` or Node equivalent); async-native core: `amap`, `astarmap`, `aapply`; `map_async`/`apply_async`/`starmap_async` return an `AsyncResult` backed by an asyncio task with `get(timeout=None)`, `wait()`, `ready()`, `successful()`
  - Sync `map`/`apply`/`starmap`: detect the runtime capability chosen in the design doc — use `pyodide.ffi.run_sync` when JSPI is available, the validated Atomics path where applicable, and otherwise raise `RuntimeError` with a message showing the exact async replacement call
  - `chunksize` support that batches items per task message (reuses `mapPickled`), preserving `multiprocessing`'s ordered-results semantics; `imap`/`imap_unordered` as async generators yielding as results arrive
  - `close`/`join`/`terminate`/`__exit__` mapped onto the JS pool lifecycle; `wasm_multiprocessing.cpu_count()` at module level

- [ ] Write the Vitest suite `tests/multiprocessing.test.ts` (reuse the driver-Pyodide fixture from `tests/helpers.ts`):
  - `amap` matches `builtins.map` results for a pure function over 20 items with `chunksize=5`
  - `apply_async(...).get()` awaited form returns the right value; `ready()` flips to True; `successful()` is False after a raising task and `get()` re-raises the original exception type
  - `imap_unordered` yields all results with completion-order allowed; `imap` preserves order
  - Context-manager usage terminates workers (verify via pool ping/status)
  - Sync `map` behavior matches the design doc for the Node environment: either it blocks and returns correctly (if the spike validated a sync path) or it raises the guidance error — assert whichever the design doc committed to
  - A small "porting" test: a snippet written for stdlib `multiprocessing` runs after only changing the import line and awaiting the entry point

- [ ] Run `npm test` (full suite — the new file plus all existing tests) and fix failures until green twice consecutively. If shim changes touched shared code in `pyodide_pool`, re-run `npm run demo:dask` to confirm no regression.

- [ ] Add the JupyterLite demo and benchmark: build the `wasm_multiprocessing` wheel into `demos/jupyterlite/files/wheels/`, create `04-multiprocessing.ipynb` (install both wheels, port a classic `multiprocessing.Pool` prime-count example with the one-line import change, show `AsyncResult` usage, and time pool sizes 1/2/4 with a bar chart), extend `03-benchmark.ipynb` with a dask-vs-multiprocessing-shim comparison on the same workload, rebuild with `npm run build:lite`, and verify by driving the new notebook end-to-end with the Playwright MCP browser tools. Extend `e2e/jupyterlite.spec.ts` with a smoke test for `04-multiprocessing.ipynb` (stable `print("MP_DEMO_OK", ...)` marker) and run it until green.

- [ ] Write the closing comparison and polish the repo entry point:
  - `docs/architecture/dask-vs-multiprocessing.md` (front matter `type: analysis`, tags `[comparison, dask, multiprocessing]`, wiki-links to both design docs and both benchmark reports) comparing the two approaches: API ergonomics, porting cost for existing code, scheduling capabilities (graphs vs flat maps), overheads measured in the benchmarks, and a recommendation for when to use which
  - Update the root `README.md`: project overview, architecture diagram (ASCII or mermaid) of driver/pool/workers, quick-start commands for every entry point (`demo:node`, `demo:dask`, `test`, `test:browser`, `bench`, `build:lite` + `serve:lite`), and a linked map of the `docs/` knowledge base
