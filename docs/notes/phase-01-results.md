---
type: note
title: Phase 01 Results — Node.js Pyodide Worker-Pool Prototype
created: 2026-07-17
tags:
  - prototype
  - benchmark
related:
  - '[[pyodide-parallelism]]'
  - '[[worker-pool-api]]'
---

# Phase 01 Results — Node.js Pyodide Worker-Pool Prototype

End-to-end verification of the Phase 01 prototype: a pool of Web Workers (via
`web-worker` over Node `worker_threads`), each hosting its own Pyodide
interpreter, executing CPU-bound Python in parallel through
`@fideus-labs/worker-pool`. See [[pyodide-parallelism]] for why in-process
threading is impossible in Pyodide and worker-level parallelism is the only
option, and [[worker-pool-api]] for the pool contract the wrapper builds on.

## Verified outcome

`npm run demo:node` (esbuild bundle + `tsx examples/node-demo.ts`) exits 0 with
matching prime counts on the serial and parallel paths. Two clean runs during
Phase 01 measured speedups of **3.17x** and **3.21x** — comfortably above the
1.5x acceptance threshold, and close to the ~3.5–3.8x practically reachable
with 8 unevenly-sized chunks on 4 workers.

## Measured timings (final run, 2026-07-17)

Workload: count primes in `[2, 4_000_000)` by pure-Python trial division,
split into 8 equal-width chunks (per-chunk cost grows ~n^1.5, so later chunks
are deliberately heavier: 0.86 s → 2.83 s).

| Metric | Serial (pool of 1) | Parallel (pool of 4) |
| --- | --- | --- |
| Interpreter boot (wall) | 1.17 s | 1.25 s for all 4 |
| Per-worker `bootMs` | 1102 ms | 1152–1185 ms |
| 8-chunk wall-clock | 16.48 s | 5.13 s |
| Total primes | 283 146 | 283 146 (= π(4 000 000) ✓) |
| **Speedup** | — | **3.21x** |

Notable: booting 4 Pyodide interpreters in parallel costs about the same wall
time as booting one (~1.2 s) — worker boots don't contend meaningfully, so
`warmup()` makes pool startup effectively O(1) in pool size. Boot is excluded
from both wall-clock numbers by warming up each pool before timing; with boot
included the parallel path would still win (6.4 s vs 17.7 s, ~2.8x).

Environment: Node v24.18.0, `pyodide` npm 314.0.2 (CPython 3.14.0), 16 logical
CPUs, Linux (WSL2).

## Workarounds required (load-bearing for later phases)

1. **Worker URL resolution under `web-worker`.** The shim resolves relative
   URLs against `process.cwd()`, not the importing module. The
   `PyodidePool` default (`new URL('pyodide-worker.js', import.meta.url)`) is
   only correct when running the built `dist/index.js`; anything running from
   source (tsx, vitest) must pass an absolute `file://` URL:
   `workerUrl: pathToFileURL(path.join(root, 'dist', 'pyodide-worker.js'))`.
2. **Pyodide WASM asset resolution inside `worker_threads`.** The worker
   bundle keeps `pyodide` **external** in esbuild (`scripts/build.mjs`), so
   `loadPyodide()` resolves its WASM/stdlib assets from `node_modules/pyodide`
   on disk. Bundling pyodide in would break that path resolution; the boot is
   sub-second from local disk anyway.
3. **Structured-clone avoidance.** The demo uses `map()` in *function* mode —
   `(chunk) => pythonSource` with `lo`/`hi` interpolated directly into the
   Python source — so nothing crosses the worker boundary except strings and
   the JSON-safe result. The worker's conversion chain (JS pass-through →
   `toJs({ dict_converter: Object.fromEntries, create_pyproxies: false })` →
   Python-`json` round-trip → error response) handled the integer results
   without ever reaching a fallback.
4. **worker-pool 1.0.0 rejection semantics.** A rejected task permanently
   removes its worker slot from the pool, so protocol tasks always *resolve*
   with the raw `WorkerResponse` and errors are unwrapped after the worker is
   recycled (`PyodideTaskError` carries `pythonTraceback`/`workerStack`).
   Also `runTasks([])` never settles — `map([])` short-circuits.
5. **Test-runner constraints** (for Phase 03): `vitest.config.ts` needs
   `pool: 'forks'` (the `web-worker` shim checks
   `worker_threads.isMainThread` at import time) and
   `fileParallelism: false` (test files race on esbuilding the same
   `dist/pyodide-worker.js`).
6. **Cross-check discipline.** The demo asserts per-chunk serial==parallel
   counts *and* the hardcoded total π(4 000 000)=283 146, exiting nonzero on
   mismatch; both pools are shut down via `terminate()` so the process exits
   cleanly with no lingering `worker_threads`.

## Implications for later phases

- The ~1.2 s parallel warmup and LIFO worker recycling mean interpreter boot
  is a one-time cost per pool, not per task — the dask scheduler backend
  (Phase 02) can assume warm workers.
- Chunk-size imbalance (workaround for equal-width ranges) is exactly the
  kind of scheduling problem dask's graph executor solves properly; the 8-on-4
  oversubscription trick is a stopgap.
- Everything above ran in Node; the browser path (Phase 04) revisits URL
  resolution (real `Worker` + bundler-served URLs) and asset loading (CDN
  instead of `node_modules`).
