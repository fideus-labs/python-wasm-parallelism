---
type: research
title: Multiprocessing on WASM — Why It Fails, Prior Art, and What Can Be Emulated
created: 2026-07-17
tags:
  - multiprocessing
  - webassembly
  - emscripten
  - pyodide
  - atomics
related:
  - '[[pyodide-parallelism]]'
  - '[[dask-schedulers]]'
  - '[[worker-pool-api]]'
---

# Multiprocessing on WASM — Why It Fails, Prior Art, and What Can Be Emulated

CPython's `multiprocessing` is built on OS primitives that simply do not
exist inside a WebAssembly sandbox. This doc records *why* each layer fails,
what others have done about it, and which parts of the `multiprocessing.Pool`
surface can be honestly emulated over message-passing workers — the API
target for later phases of this project.

## Why `multiprocessing` fails under Emscripten/Pyodide

The failure is layered — every mechanism `multiprocessing` could use is gone:

- **No processes.** A browser tab is a single OS process; Emscripten
  implements no `fork()`/`exec()` (`popen` is a stub —
  [emscripten#3819](https://github.com/emscripten-core/emscripten/pull/3819)),
  and its own pthreads docs state *"The Emscripten implementation does also
  not support multiprocessing via fork() and join()"*
  ([pthreads docs](https://emscripten.org/docs/porting/pthreads.html)).
  Multiprocess support appears only under "future features" in the WASM
  design docs. Pyodide maintainers are blunt:
  *"a single browser tab runs in a single process. So I don't think
  multiprocessing will be possible even in the future"*
  ([pyodide discussion #4623](https://github.com/pyodide/pyodide/discussions/4623)).
- **No threads either** (the `ThreadPool`/`multiprocessing.dummy` fallback):
  Pyodide is built without `-pthread` (dynamic-linking conflict, detailed in
  [[pyodide-parallelism]]), so `threading.Thread.start()` raises
  `RuntimeError: can't start new thread`
  ([FAQ](https://pyodide.org/en/stable/usage/faq.html)).
- **Missing C substrate.** The `_multiprocessing` extension module isn't
  built, and POSIX semaphores don't exist, so even importing pieces fails:
  `ImportError: This platform lacks a functioning sem_open implementation`
  ([pyodide#1603](https://github.com/pyodide/pyodide/issues/1603)).
- Officially, `multiprocessing`, `threading`, and `sockets` are "included but
  not working"
  ([wasm-constraints](https://pyodide.org/en/stable/usage/wasm-constraints.html));
  the sanctioned feature-detect is `sys.platform == "emscripten"` /
  `sys._emscripten_info.pthreads == False`, and well-behaved libraries fall
  back to `n_threads = 1`.

The closing note on pyodide#1603 frames this project's thesis exactly:
*"The underlying issue can't be otherwise fixed (**until maybe someone
implements multiprocessing on top of the webworker API**)."*

## Prior art

### joblib / scikit-learn: degrade to serial

The ecosystem's mainstream answer is graceful degradation, not emulation:

- Pyodide shipped ancient joblib 0.11 for years because the loky backend
  dragged in `multiprocessing` imports at module load;
  [joblib#1246](https://github.com/joblib/joblib/pull/1246) (by Pyodide
  maintainer hoodmane) added a missing-`_multiprocessing` CI environment so
  newer joblib stays importable.
- joblib 1.5.0 regressed (`KeyError: 'loky'` when `prefer='processes'` on
  Pyodide, [joblib#1720](https://github.com/joblib/joblib/issues/1720));
  the fix ([#1721](https://github.com/joblib/joblib/pull/1721)) makes joblib
  silently substitute the threading backend when multiprocessing is disabled
  — which on Pyodide means **sequential** execution.
- scikit-learn treats WASM as a supported-but-serial platform
  ([sklearn#23727](https://github.com/scikit-learn/scikit-learn/issues/23727)):
  `n_jobs` is effectively 1. Nobody in this lineage runs anything in
  parallel — they just avoid crashing. Actual parallelism needs workers.

### Sync-over-worker bridges: the Atomics.wait lineage

A separate lineage builds **synchronous facades over async workers** — the
key ingredient for a blocking `Pool.map`:

- [synclink](https://github.com/pyodide/synclink) (hoodmane's fork of
  Google's Comlink): async dispatch plus a `.syncify()` that blocks the
  calling worker with `Atomics.wait` on a `SharedArrayBuffer` until another
  thread writes the result and `Atomics.notify`s. Built specifically for
  Pyodide.
- [pyodide#1545](https://github.com/pyodide/pyodide/issues/1545) /
  [#1504](https://github.com/pyodide/pyodide/issues/1504) /
  [#1219](https://github.com/pyodide/pyodide/issues/1219) survey the three
  known blocking strategies: (1) `Atomics.wait` on SAB — cleanest, needs
  cross-origin isolation; (2) synchronous XHR intercepted by a Service
  Worker — works without SAB, more moving parts; (3)
  [unthrow](https://github.com/joemarshall/unthrow)-style restartable
  execution ("dark magic").
- [sync-message](https://github.com/alexmojaki/sync-message),
  [comsync](https://github.com/alexmojaki/comsync), and
  [pyodide-worker-runner](https://github.com/alexmojaki/pyodide-worker-runner)
  (alexmojaki) package strategy (1) with a Service-Worker fallback; used in
  production by futurecoder and papyros.
- Pyodide itself uses the SAB channel for `setInterruptBuffer` (delivering
  `KeyboardInterrupt` into a busy worker) — the same mechanism our pool can
  use for cancellation later.

These bridges solve *synchronous waiting*; none of them provide a
`multiprocessing.Pool`-shaped API over a pool of Python interpreters — that
is the gap this project fills, using [[worker-pool-api]] for the pooling.

## What can be emulated over message-passing workers

The honest mapping of `multiprocessing.Pool` onto N Pyodide workers
(one interpreter each, structured-clone messaging — see
[[pyodide-parallelism]]):

| `multiprocessing` surface | Emulation over workers | Fidelity |
|---|---|---|
| `Pool(processes=N)` | pool of N workers | ✅ direct |
| `initializer=` / `initargs=` | run per worker at first boot (cloudpickled) | ✅ direct |
| `map_async` / `apply_async` / callbacks / `imap` | task dispatch → promise; `AsyncResult` wraps it; `imap` as async iterator | ✅ natural — async is the native shape |
| `AsyncResult.get(timeout)` | `await` (async ctx) or Atomics/JSPI block (see below) | ✅ / ⚠️ context-dependent |
| blocking `map` / `apply` / `starmap` | requires a legal blocking primitive on the calling thread | ⚠️ Node main thread + any worker: yes; browser main thread: async-only or JSPI |
| `maxtasksperchild` | worker recycling policy (recreate after K tasks) | ✅ pool-level |
| `close` / `join` / `terminate` | drain via `onIdle()` / `terminateWorkers()` | ✅ approximate ([[worker-pool-api]]) |
| argument/result passing | pickle/cloudpickle bytes over `postMessage` | ✅ same pickling contract as real `multiprocessing` |
| fork semantics (child inherits parent state) | ❌ none — workers boot fresh; all state ships explicitly (initializer or per-task) | ❌ document loudly |
| `Value` / `Array` / `shared_memory` / `Lock` / `Queue` / `Pipe` between processes | SAB-based emulation is conceivable but out of scope | ❌ initially unsupported |

Two semantic gaps deserve emphasis:

1. **No copy-on-write inheritance.** Real `fork()` gives children the
   parent's globals for free. Our workers start empty (multi-second boot,
   see [[pyodide-parallelism]]) — so the API must push users toward
   `initializer` + self-contained cloudpickled functions, and the pool must
   recycle warm interpreters to amortize boot.
2. **Everything crosses a serialization boundary** — same as real
   multiprocessing (which pickles too), so code that works with `Pool`
   is usually already pickle-clean. cloudpickle (in the Pyodide
   distribution) extends coverage to lambdas/closures — see
   [[dask-schedulers]].

## Where synchronous blocking is possible

The blocking rules (MDN `Atomics.wait`; verified in
[[pyodide-parallelism]]) determine where a *blocking* `Pool.map` facade can
exist:

| Context | `Atomics.wait` (block) | Consequence for a sync API |
|---|---|---|
| Browser main thread | ❌ throws (only `waitAsync`) | async API only — or JSPI |
| Browser worker | ✅ (needs COOP/COEP for SAB) | sync facade OK |
| Node main thread | ✅ (SAB unrestricted) | sync facade OK |
| Node worker thread | ✅ | sync facade OK |

- Design consequence: the **async API is the portable core**; a synchronous
  wrapper is an add-on for contexts in the ✅ rows, implemented as: post
  task → `Atomics.wait` on a SAB mailbox → dispatcher thread writes result
  + `Atomics.notify`. (Note the result itself must fit the SAB protocol or
  be relayed in a follow-up message after wake-up.)
- **JSPI is the future path that removes the table entirely**:
  `pyodide.ffi.run_sync(awaitable)` suspends the WASM stack until the
  promise resolves — a blocking-looking `Pool.map` on the browser main
  thread with no SAB — but it needs Chrome 137+/Node 24 flag and has known
  crash caveats (pyodide#6106). Details in [[pyodide-parallelism]].
- This is also exactly why the dask work in [[dask-schedulers]] centers on
  an async graph executor with sync facades layered per-context.

## Architectural conclusions for this project

1. `multiprocessing` cannot be fixed *inside* one WASM instance — no
   processes, no threads, no semaphores. Emulation must live **above** the
   interpreter, across a pool of workers ([[worker-pool-api]]).
2. Mirror the `Pool` API asymmetrically: async methods (`map_async`,
   `apply_async`, `imap`) are first-class everywhere; blocking methods exist
   only where the platform permits (Node, workers) or via JSPI later.
3. Be explicit about non-goals: no fork-style state inheritance, no shared
   `Value`/`Lock`/`Queue` objects in v1; initializer + cloudpickle carry
   state instead.
4. Prior art validates the pieces — Atomics.wait sync bridges (synclink,
   sync-message) and worker-per-interpreter pools — but no one has shipped
   the combination as a `multiprocessing.Pool` work-alike; the joblib/sklearn
   lineage just degrades to serial.
5. Reuse the pool's recycling/warmup ([[worker-pool-api]]) to make
   `Pool()` construction cost resemble fork-based expectations as closely
   as WASM allows.
