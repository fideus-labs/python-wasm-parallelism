---
type: research
title: Pyodide Parallelism — Workers, Node.js, SharedArrayBuffer, and JSPI
created: 2026-07-17
tags:
  - pyodide
  - webassembly
  - web-workers
  - node
  - parallelism
related:
  - '[[worker-pool-api]]'
  - '[[multiprocessing-on-wasm]]'
  - '[[dask-schedulers]]'
---

# Pyodide Parallelism — Workers, Node.js, SharedArrayBuffer, and JSPI

Pyodide is a CPython distribution compiled to WebAssembly with Emscripten. A single
Pyodide instance is **strictly single-threaded**: the interpreter runs on whatever
JS thread loaded it, and it cannot spawn OS threads or processes. The only way to
run Python in parallel is to boot **multiple independent interpreters, one per
Web Worker (or Node `worker_threads` thread)**, and coordinate them over message
passing. That is exactly the architecture this project builds on top of
[[worker-pool-api]].

## Pyodide in Web Workers

- The official pattern ([pyodide.org webworker docs](https://pyodide.org/en/stable/usage/webworker.html))
  is: the worker script calls `loadPyodide()` once at module scope
  (`let pyodideReadyPromise = loadPyodide()`), and `self.onmessage` awaits that
  promise before executing incoming code with `pyodide.runPythonAsync`.
- Pyodide requires a **module-type worker** (`new Worker(url, { type: "module" })`)
  because `pyodide.asm.mjs` is an ES module; classic workers using
  `importScripts()` are not supported in current versions.
- **No sharing between contexts.** Pyodide does not support sharing the Python
  interpreter, its globals, or loaded packages between workers or with the main
  thread. Each worker gets its own VM, its own Emscripten filesystem, its own
  `sys.modules`. Communication is by structured clone (`postMessage`) only.
- Each worker is itself single-threaded: only one Python script executes at a
  time per worker. Parallelism therefore comes only from having N workers.
- The docs' recommended request/response pattern attaches an `id` to each
  message and matches responses on that `id` — the same one-request/one-response
  protocol our `pyodide-worker.ts` implements.

### Cost model: interpreter boot is the expensive part

`loadPyodide()` fetches/instantiates a ~10–20 MB WASM module plus the Python
stdlib; boot takes on the order of **1–5 seconds** per worker (slower on first
load without cache). Loading extra packages (`pyodide.loadPackage`,
`micropip.install`) adds more. Consequences:

- Worker (and interpreter) **reuse across tasks is essential** — this is why the
  LIFO worker recycling in [[worker-pool-api]] matters so much: a recycled worker
  keeps its booted interpreter in module scope and skips the boot entirely.
- A pool should support **warmup** (boot all interpreters in parallel before
  timing-sensitive work) so that per-task latency reflects execution, not boot.

## Pyodide in Node.js

- The `pyodide` npm package officially supports **Node ≥ 18** (Node < 18 was
  dropped in Pyodide 0.25.0). Usage is the same API:
  `import { loadPyodide } from "pyodide"; const py = await loadPyodide();`.
- In Node, `loadPyodide()` **resolves the bundled WASM and stdlib assets from
  `node_modules/pyodide`** (see `src/js/compat.ts` / `initNodeModules` in the
  Pyodide sources) — no CDN or `indexURL` needed, though `indexURL` can still be
  passed to point at a different distribution. This matters inside
  `worker_threads`: the worker bundle must either keep `pyodide` as an external
  import (so Node module resolution finds the assets) or pass an explicit
  `indexURL`.
- Node has no built-in `Worker` global with the Web Worker API; it has
  `worker_threads`. The [`web-worker`](https://www.npmjs.com/package/web-worker)
  npm package polyfills the browser `Worker` API on top of `worker_threads`,
  which lets the same pool code run in both environments. Caveat: worker URL
  resolution under the polyfill typically needs a file path or `file://` URL
  rather than bundler-style `new URL('./x.js', import.meta.url)` resolution.
- Node's main thread **is allowed to block** (e.g. `Atomics.wait` works on the
  Node main thread), unlike the browser main thread — relevant for future
  synchronous-bridge designs, see [[multiprocessing-on-wasm]].

## Why Pyodide has no native threads or fork

- **No pthreads.** Pyodide is deliberately built **without** Emscripten's
  `-pthread` support: "The interaction between pthreads and dynamic linking is
  slow and buggy" ([Pyodide ABI docs](https://pyodide.org/en/stable/development/abi/flags.html)).
  Pyodide relies heavily on dynamic linking for its package ecosystem, and
  Emscripten cannot reliably combine dynamic linking with pthreads
  (tracked since [pyodide#237](https://github.com/pyodide/pyodide/issues/237),
  emscripten-core/emscripten#3494). Packages compiled with `-pthread` will not
  even load against the Pyodide ABI.
- **No fork/processes.** Emscripten implements no `fork()`/`exec()` — a WASM
  instance is a single linear-memory sandbox with no notion of OS processes.
  Emscripten's own pthreads docs state multiprocessing via `fork()` is
  unsupported.
- As a result the stdlib modules `threading`, `multiprocessing`, and `sockets`
  **can be imported but are not functional** in Pyodide
  ([wasm-constraints docs](https://pyodide.org/en/stable/usage/wasm-constraints.html)).
  `sys._emscripten_info.pthreads` is `False`; well-behaved libraries check this
  and fall back to `n_threads = 1`. Details and emulation strategy in
  [[multiprocessing-on-wasm]].

## SharedArrayBuffer and COOP/COEP

`SharedArrayBuffer` (SAB) is the only shared-memory primitive between JS threads,
and it is security-gated in browsers:

- The page must be **cross-origin isolated**, i.e. served with
  `Cross-Origin-Opener-Policy: same-origin` and
  `Cross-Origin-Embedder-Policy: require-corp`. Without these headers,
  `SharedArrayBuffer` is simply not defined.
- **Node.js has no such restriction** — SAB and `Atomics` work out of the box,
  including on the main thread.
- `Atomics.wait` (blocking) works **inside workers** and on **Node's main
  thread**, but throws on the **browser main thread** (only `Atomics.waitAsync`
  is allowed there). This asymmetry is the crux of sync/async bridging: a worker
  may block waiting for another thread; a browser page may not.
- Pyodide itself uses SAB optionally, e.g. `pyodide.setInterruptBuffer(sab)`
  lets the main thread signal (KeyboardInterrupt) a Python interpreter running
  in a worker.
- For this project's message-passing prototype SAB is **not required** — results
  travel by structured clone. SAB becomes relevant for zero-copy numeric
  payloads and for synchronous call bridges (see [[multiprocessing-on-wasm]]).

## JSPI and `pyodide.ffi.run_sync` — the future sync/async bridge

JavaScript Promise Integration (JSPI) is a WebAssembly feature that lets a WASM
call stack **suspend** until a JS `Promise` resolves — synchronous-looking code
over async APIs, without SAB or busy-waiting.

- Pyodide exposes this as **`pyodide.ffi.run_sync(awaitable)`** — "block until an
  awaitable is resolved" — integrated with Pyodide's event loop since **0.27.7**
  ([Pyodide JSPI blog post, June 2025](https://blog.pyodide.org/posts/jspi/)).
- Stack switching only works when Python was entered asynchronously: via
  `pyodide.runPythonAsync()`, by calling an async Python function, or via
  `PyCallable.callPromising()`. Under plain `runPython()`, `run_sync` raises
  `RuntimeError: Cannot stack switch`. Query availability with
  `pyodide.ffi.can_run_sync()`.
- Runtime support (as of the 2025 blog post): **Chrome 137+ ships JSPI by
  default**; Node 24 needs `--experimental-wasm-jspi`; Firefox behind
  `javascript.options.wasm_js_promise_integration`. Safari: not yet.
- Stability caveat: JSPI + `runPythonAsync` has caused silent
  `STATUS_ACCESS_VIOLATION` crashes on some machines with heavy asyncio loads
  ([pyodide#6106](https://github.com/pyodide/pyodide/issues/6106)); the
  workaround is `loadPyodide({ enableRunUntilComplete: false })` or deleting
  `WebAssembly.Suspending`/`WebAssembly.promising` before load. Treat JSPI as a
  promising-but-not-yet-boring dependency.
- Relevance here: JSPI is the future path for letting a **blocking**
  `multiprocessing.Pool`-style API inside one Pyodide interpreter await results
  from sibling workers without SAB gymnastics — see [[multiprocessing-on-wasm]]
  and [[dask-schedulers]] for the scheduler-side implications.

## Architectural conclusions for this project

1. Parallel Python-in-WASM = **pool of N workers × 1 Pyodide interpreter each**,
   coordinated from JS. No shared interpreter state; ship code + data per task.
2. Interpreter boot (~seconds) dominates; **recycling and warmup are mandatory**
   for any credible speedup measurement — precisely what
   [[worker-pool-api]]'s LIFO recycling provides.
3. Node is the easiest prototype target: no COOP/COEP hurdles, `pyodide` npm
   resolves its own assets, `web-worker` bridges the API gap to the browser.
4. Results must be structured-clone-safe: convert Python objects with
   `.toJs()` + dict converters or JSON round-trip before `postMessage`.
5. Sync bridges (needed later for dask/multiprocessing emulation) can use
   `Atomics.wait` in workers today and JSPI (`run_sync`) as browsers/Node catch
   up.
