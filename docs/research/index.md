---
type: reference
title: Research Index — Pyodide Worker-Pool Parallelism
created: 2026-07-17
tags:
  - index
  - pyodide
  - parallelism
related:
  - '[[pyodide-parallelism]]'
  - '[[worker-pool-api]]'
  - '[[dask-schedulers]]'
  - '[[multiprocessing-on-wasm]]'
---

# Research Index — Pyodide Worker-Pool Parallelism

Phase-01 research for running CPU-bound Python in parallel under WebAssembly:
a pool of Web Workers (or Node `worker_threads`), each hosting its own Pyodide
interpreter, coordinated over message passing — with dask and a
`multiprocessing.Pool` work-alike layered on top in later phases.

## The four documents

- [[pyodide-parallelism]] — the platform ground truth: Pyodide in Web Workers
  and Node.js, why one interpreter is strictly single-threaded (no
  `-pthread`, no `fork`), the multi-second boot cost model,
  SharedArrayBuffer/COOP-COEP rules, the `Atomics.wait` asymmetry, and JSPI /
  `pyodide.ffi.run_sync` as the future sync/async bridge.
- [[worker-pool-api]] — the `@fideus-labs/worker-pool` package we build on:
  the `(worker | null) => Promise<{ worker, result }>` task contract,
  `add()`/`onIdle()` and `runTasks()` with progress + cancellation, and why
  LIFO worker recycling amortizes Pyodide's boot cost.
- [[dask-schedulers]] — dask's pluggable scheduler seam (`get(dsk, keys)`),
  the `dask.local` state machine and its one blocking primitive, why a
  blocking scheduler deadlocks the browser main thread, the async graph
  executor design on Pyodide's event loop, dask-via-micropip availability,
  and cloudpickle for shipping callables.
- [[multiprocessing-on-wasm]] — why CPython `multiprocessing` fails at every
  layer under Emscripten, prior art (joblib/sklearn's degrade-to-serial;
  synclink/sync-message Atomics bridges), which parts of the `Pool` API can
  be emulated over message-passing workers, and where synchronous blocking
  is legal per platform context.

## How they fit together

The platform constraints ([[pyodide-parallelism]]) force a
worker-per-interpreter architecture; [[worker-pool-api]] supplies the pooling
and recycling; [[dask-schedulers]] and [[multiprocessing-on-wasm]] map two
familiar Python parallelism APIs onto that substrate, both converging on the
same rule: **async message-passing is the portable core, synchronous facades
are per-context add-ons (Atomics.wait today, JSPI tomorrow)**.
