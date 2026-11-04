---
type: research
title: '@fideus-labs/worker-pool API — Contract, Recycling, and Batch Execution'
created: 2026-07-17
tags:
  - worker-pool
  - web-workers
  - typescript
  - api
related:
  - '[[pyodide-parallelism]]'
---

# @fideus-labs/worker-pool API

Source: [fideus-labs/worker-pool README](https://raw.githubusercontent.com/fideus-labs/worker-pool/main/README.md)
(package `@fideus-labs/worker-pool`, MIT, zero runtime dependencies). It is a
Web Worker pool with **bounded concurrency** — at most `poolSize` workers run
simultaneously — plus worker recycling, a `ChunkQueue`-style interface
(`add()`/`onIdle()`), and a batch interface (`runTasks()`) with progress
reporting and cancellation. A companion package `@fideus-labs/fizarrita`
offloads zarrita.js codec work through the same pool (not needed here, but it
demonstrates the intended usage pattern).

## The task function contract

The pool never creates workers itself. Every task function receives an
available worker **or `null`**, and must return both the worker (for
recycling) and the result. Workers are typed as `WorkerLike` — the
`postMessage`/`terminate`/`addEventListener`/`removeEventListener` slice that
a browser `Worker` and worker-pool's Node `NodeWorker` both satisfy:

```typescript
type WorkerPoolTask<T> = (
  worker: WorkerLike | null
) => Promise<{ worker: WorkerLike; result: T }>
```

- `worker === null` → the pool wants a **new worker created by the task**:
  `const w = worker ?? createWorker(workerUrl)` (a module `Worker` in
  browsers, a `NodeWorker` over `worker_threads` in Node). Bundlers only
  detect a worker entry point from a literal
  `new Worker(new URL('./w.js', import.meta.url), { type: 'module' })`, so a
  bundled browser app must ship the worker asset some other way (a `?worker`
  import, a copied asset) and pass its URL — which is what this project does.
- `worker !== null` → a **recycled** worker; reuse it as-is.
- The task installs its own message listener, posts its request, and
  resolves `{ worker: w, result }` when the reply arrives. Because each task
  attaches (and detaches) an id-matched listener itself, the natural worker
  protocol is **one request → one response per task**. Handle `'error'` too,
  or a failing worker leaves `onIdle()`/`runTasks()` pending forever.

Canonical task factory from the README:

```typescript
function createTask(input: number) {
  return async (worker: WorkerLike | null) => {
    const w = worker ?? createWorker(workerUrl)
    const result = await new Promise<number>((resolve, reject) => {
      const onMessage = (e: { data: number }) => {
        detach()
        resolve(e.data)
      }
      const onError = (e: { message: string }) => {
        detach()
        reject(new Error(e.message))
      }
      const detach = () => {
        w.removeEventListener('message', onMessage)
        w.removeEventListener('error', onError)
      }
      w.addEventListener('message', onMessage)
      w.addEventListener('error', onError)
      w.postMessage(input)
    })
    return { worker: w, result }
  }
}
```

## API surface

| Member | Signature | Notes |
|---|---|---|
| constructor | `new WorkerPool(poolSize: number)` | At most `poolSize` concurrent workers. |
| `add` | `add<T>(fn: WorkerPoolTask<T>): void` | Enqueue a task. Tasks are **started when `onIdle()` is called**. |
| `onIdle` | `onIdle<T>(): Promise<T[]>` | Execute all enqueued tasks, resolve with results **in `add()` order**. |
| `runTasks` | `runTasks<T>(taskFns, progressCallback?): { promise, runId }` | Batch submit; `promise` resolves with ordered results; `progressCallback(completed, total)` fires after each task. |
| `cancel` | `cancel(runId: number): void` | Cancel a pending `runTasks` batch; its promise **rejects** with `'Remaining tasks canceled'`. |
| `terminateWorkers` | `terminateWorkers(): void` | Terminates all **idle** workers. The pool remains usable — new workers are created (by tasks) as needed. |

Two usage styles:

- **ChunkQueue style** (`add` + `onIdle`) — compatible with zarrita.js /
  p-queue patterns; fire-and-collect.
- **Batch style** (`runTasks`) — what our `PyodidePool.map()` should wrap,
  since it gives progress callbacks and a `runId` handle for cancellation.

## Key insight: LIFO worker recycling amortizes Pyodide boot

Workers are reused **LIFO** across tasks instead of being re-created. For
generic codec workers this just saves worker spawn overhead (~ms). For Pyodide
workers it is transformative: a recycled worker's module scope still holds a
**booted Python interpreter** (multi-second `loadPyodide()` cost) plus all
already-loaded packages. With recycling:

- Boot cost is paid **once per pool slot**, not once per task — 8 tasks on a
  4-worker pool boot 4 interpreters, not 8.
- LIFO order keeps the **most recently used** (hottest, most-packages-loaded)
  interpreter in rotation, and lets rarely-needed extra workers go idle where
  `terminateWorkers()` can reap them.
- A **warmup** step (dispatch `poolSize` trivial tasks, e.g. pings) forces all
  interpreters to boot in parallel before benchmarking, so measured task time
  is execution time, not boot time. See [[pyodide-parallelism]] for the boot
  cost model.

## Design implications for `PyodidePool`

1. **Do not reimplement queuing** — wrap `WorkerPool` directly: `runPython()`
   → one `add()`/`onIdle()` (or single-element `runTasks`), `map()` →
   `runTasks()` exposing `(completed, total)` progress and `cancel(runId)`.
2. The task factory owns worker creation, so **environment-awareness lives
   there**: worker-pool's `createWorker` returns the global `Worker` in
   browsers and its `NodeWorker` (over `worker_threads`) in Node — same
   contract.
3. Because each task rebinds message handling, use a **unique message `id` per
   task** and match the response on that `id` (guards against stray messages
   from a recycled worker's previous life).
4. `terminateWorkers()` only reaps idle workers and the pool stays alive —
   `PyodidePool.terminate()` should call it after draining, and callers must
   know a subsequent task will pay a fresh interpreter boot.
5. Results must be structured-clone-safe before `postMessage` — conversion
   happens **inside the worker**, see [[pyodide-parallelism]].

## SharedArrayBuffer notes (from the fizarrita half of the README)

The pool itself is transport-agnostic, but the companion package shows the SAB
pattern: with COOP/COEP headers set (`Cross-Origin-Opener-Policy: same-origin`,
`Cross-Origin-Embedder-Policy: require-corp`), workers can decode directly into
a `SharedArrayBuffer`-backed output, eliminating one transfer + one copy per
chunk; without the headers, SAB use throws a descriptive error. The same
technique would apply later to shipping large numeric results out of Pyodide
workers without copies ([[pyodide-parallelism]] covers the COOP/COEP and
Atomics rules).
