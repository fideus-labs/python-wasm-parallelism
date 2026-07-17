---
type: research
title: Dask Schedulers — Pluggable get(), dask.local Internals, and an Async Executor for Pyodide
created: 2026-07-17
tags:
  - dask
  - scheduler
  - task-graph
  - pyodide
  - async
related:
  - '[[pyodide-parallelism]]'
  - '[[multiprocessing-on-wasm]]'
  - '[[worker-pool-api]]'
---

# Dask Schedulers — Pluggable `get()`, `dask.local` Internals, and an Async Executor for Pyodide

Dask separates **graph construction** (collections like `dask.array`,
`dask.delayed` build a plain dict-of-tasks) from **graph execution** (a
scheduler consumes the graph). That separation is the hook this project needs:
we can keep dask's graph-building front end and swap in our own executor that
runs each task on a [[worker-pool-api]] of Pyodide workers.

## The pluggable scheduler interface

The entry point for every scheduler is a **`get` function**
([scheduler overview](https://docs.dask.org/en/stable/scheduler-overview.html)):

```python
get(dsk: Mapping[Key, Any], keys: Key | list[Key], **kwargs) -> results
```

- `dsk` is the task graph — a dict mapping keys to tasks. Modern dask
  (≥ 2024.12) expresses tasks with `dask.Task` / `TaskRef` / `DataNode`
  objects; the legacy form is a tuple whose first element is a callable
  (`('z', (add, 'x', 'y'))`). The scheduler substitutes each task's
  references with computed values before calling the function
  ([graph spec](https://docs.dask.org/en/latest/spec.html)).
- `keys` may be a single key or a (possibly nested) list; `get` must return
  results with matching shape.
- Built-in gets: `dask.get` (synchronous), `dask.threaded.get` (thread pool,
  default for arrays/dataframes), `dask.multiprocessing.get` (process pool),
  `distributed.Client.get`.

Selection is fully pluggable
([scheduling docs](https://docs.dask.org/en/stable/scheduling.html)): the
`scheduler=` argument to `.compute()` (or `dask.config.set(scheduler=...)`)
accepts a string name **or any callable with the `get` signature** — dask's
own docs and the
[custom-collections protocol](https://docs.dask.org/en/latest/custom-collections.html)
(`__dask_scheduler__`, a static `get`-shaped method) make "anything that can
receive a task graph and a list of keys" a valid scheduler. Collections also
expose `__dask_graph__()` / `__dask_keys__()` / `__dask_postcompute__()`, so a
custom executor can bypass `.compute()` entirely: extract the graph, execute
it however it likes, then apply the finalize function to the results.

One constraint matters for us: **`compute()` calls the scheduler
synchronously and expects concrete results back**. The protocol has no
async variant — which is exactly the friction point on the browser main
thread (below).

## How `dask.local` drives a graph

`dask/local.py` (docstring: *"Asynchronous Shared-Memory Scheduler for Dask
Graphs"*) is the single-machine engine behind both the synchronous and
threaded schedulers. Its architecture is a **pure-data state machine plus a
blocking event loop**:

- `start_state_from_dask(dsk, cache)` builds the state dict:
  `dependencies` (task → prerequisites), `dependents` (reverse edges),
  `waiting` (tasks blocked on unfinished deps), `waiting_data` (which
  dependents still need each result, for memory release), `ready` (stack of
  runnable tasks), `cache` (computed values), and `running`/`finished`/
  `released` bookkeeping sets.
- `get_async(submit, num_workers, dsk, result, cache=None, ...)` is the main
  loop. Each iteration: `fire_tasks()` pops ready tasks and hands batches to
  the executor's `submit`; then the scheduler **blocks on `queue.get()`**
  waiting for a completed batch; `finish_task()` moves the task to finished,
  promotes newly unblocked dependents into `ready`, and releases cache
  entries whose `waiting_data` drained (this eager release is why the local
  scheduler is memory-frugal).
- `execute_task` wraps the user function so exceptions are packed and
  re-raised on the scheduler side; `dumps`/`loads` hooks default to identity
  (shared memory) but exist precisely so results can cross a serialization
  boundary.
- `get_sync` is just `get_async` with a `SynchronousExecutor` whose
  `submit()` runs the function inline — same state machine, zero threads.

The takeaway: **all the graph logic (readiness tracking, ordering, memory
release) is synchronous data-structure manipulation that runs fine under
Pyodide**. The only thing that does not translate is the *waiting primitive* —
`queue.get()`.

## Why a blocking scheduler cannot run on the browser main thread

Suppose `get` dispatches tasks to Pyodide workers via `postMessage` and then
blocks (a `queue.get()`-equivalent) until results arrive:

- Worker replies are delivered as JS events, which only fire **when the JS
  event loop is allowed to turn**. A synchronously blocking scheduler never
  yields, so the completion messages can never be delivered. This is a
  **deadlock**, not mere UI jank.
- The blocking escape hatch, `Atomics.wait`, is **banned on the browser main
  thread** (only `Atomics.waitAsync` is allowed there) — see the Atomics
  asymmetry table in [[pyodide-parallelism]] and the blocking analysis in
  [[multiprocessing-on-wasm]].
- Note the contrast: `dask.get` (fully synchronous, tasks run in-interpreter)
  *does* work on the main thread — it just freezes the tab while computing.
  The impossibility is specific to schedulers that **wait for work happening
  on another thread**.

Where blocking *is* legal (Node's main thread; any worker), a blocking
scheduler over SharedArrayBuffer mailboxes is feasible — but a portable
design should not depend on it.

## The async-scheduler approach

The portable answer is to **re-implement `get_async`'s loop as a coroutine**
driven by Pyodide's event loop (Pyodide's `WebLoop` schedules Python
coroutines on the JS microtask queue, so Python `await` interoperates with JS
promises natively):

```python
async def async_get(dsk, keys):
    state = start_state_from_dask(dsk)          # reuse dask's state machine
    pending: set[asyncio.Future] = set()
    while state['waiting'] or state['ready'] or pending:
        while state['ready'] and len(pending) < pool_size:
            pending.add(dispatch(state))         # postMessage → JS Promise
        done, pending = await asyncio.wait(      # replaces queue.get()
            pending, return_when=asyncio.FIRST_COMPLETED)
        for fut in done:
            finish_task(state, fut)              # reuse dask's bookkeeping
    return nested_get(keys, state['cache'])
```

- `dispatch` serializes the task (see cloudpickle below), posts it to a
  worker from the [[worker-pool-api]] pool, and wraps the reply promise as an
  asyncio future.
- Because `compute(scheduler=...)` demands a synchronous callable, the async
  executor is surfaced as **`await pyodide_compute(collection)`** (extract
  graph via `__dask_graph__`/`__dask_keys__`, run `async_get`, apply
  `__dask_postcompute__`) rather than through `scheduler=`. A synchronous
  `scheduler=` facade can be layered on later where blocking is legal
  (Node, workers) or via JSPI `run_sync` — see [[pyodide-parallelism]].
- Per-task granularity note: each dispatch ships code + pickled inputs by
  message passing, so task overhead is milliseconds, not the ~50 µs of the
  threaded scheduler. Graphs need coarse tasks (chunk sizes worth ≥ tens of
  milliseconds of compute) to amortize it.

## Dask availability in Pyodide

Verified against the Pyodide 0.28.2 lockfile (`pyodide-lock.json`, 343
packages):

- **dask is not in the Pyodide distribution** (no `dask` recipe in
  [pyodide-recipes](https://github.com/pyodide/pyodide-recipes) either).
  But dask *core* is a pure-Python wheel, so `micropip.install("dask")`
  resolves it from PyPI. Its import-time dependencies are mostly
  in-distribution — **cloudpickle 3.1.1, fsspec 2025.3.2, toolz 1.0.0**,
  pyyaml, click, packaging — and the rest (e.g. `partd`, `locket`) are pure
  wheels micropip fetches from PyPI.
- `distributed` (the distributed scheduler) is **not usable**: it needs real
  sockets and threads, both non-functional under Pyodide
  ([wasm-constraints](https://pyodide.org/en/stable/usage/wasm-constraints.html)).
  Plan around dask core + custom executor only.
- `dask.multiprocessing.get` fails for the reasons documented in
  [[multiprocessing-on-wasm]]; `dask.threaded.get` degrades to serial at
  best (`ThreadPoolExecutor` cannot spawn threads). The only stock scheduler
  that works is the synchronous one — parallelism requires our executor.

## cloudpickle for shipping callables

Each pool worker is an isolated interpreter (see [[pyodide-parallelism]]), so
task functions must cross by serialization:

- Plain `pickle` serializes functions **by reference** (module + qualname),
  which fails for lambdas, closures, and anything defined in `__main__` —
  i.e. nearly everything in a dask graph built interactively.
  **cloudpickle serializes by value**, exactly why dask.distributed uses it
  between client and workers.
- `cloudpickle` **is in the Pyodide distribution** (3.1.1 in 0.28.2;
  `pyodide.loadPackage("cloudpickle")`), so both scheduler-side and
  worker-side interpreters can use it.
- Transport: `cloudpickle.dumps(task)` yields `bytes`, which convert to a
  structured-clone-friendly buffer for `postMessage` (zero-copy transferable
  `ArrayBuffer` if desired); the worker calls `cloudpickle.loads` and
  executes. Version skew is a non-issue here — every worker runs the
  identical Pyodide build, cloudpickle's usual cross-version caveat doesn't
  bite.
- Cache values that feed downstream tasks on *other* workers must round-trip
  the same way; keeping a task's dependents on the worker that computed its
  inputs (locality-aware dispatch) is a later-phase optimization.

## Architectural conclusions for this project

1. Dask's scheduler seam is a documented, stable extension point: a `get`
   callable, or better, direct use of `__dask_graph__`/`__dask_keys__`/
   `__dask_postcompute__` behind an async `pyodide_compute()` helper.
2. Reuse `dask.local`'s state machine (`start_state_from_dask`,
   `finish_task`) verbatim; replace only the blocking `queue.get()` with
   `await asyncio.wait(..., FIRST_COMPLETED)` on Pyodide's event loop.
3. A blocking `get` deadlocks the browser main thread by construction; async
   is the portable core, with sync facades (Node / workers / JSPI) layered
   on top later.
4. Install dask core via micropip (not in-distribution); never depend on
   `distributed`.
5. Ship callables with cloudpickle (in-distribution) over `postMessage`;
   design graphs with coarse tasks to amortize per-message overhead.
