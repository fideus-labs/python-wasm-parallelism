"""Async dask scheduler executing task graphs on the Pyodide worker pool.

This module runs in the MAIN Pyodide instance (the driver). It implements
the design-doc algorithm (docs/architecture/dask-scheduler-design.md): a
Kahn-style in-degree executor over the raw graph dict that dispatches every
ready task to the worker pool at once and, as each completes, decrements its
dependents and dispatches the newly ready ones. Concurrency is bounded by
the JS pool's size, never by the scheduler — over-dispatching just fills the
pool queue, which is exactly what utilization wants.

dask is imported lazily inside :func:`get`/:func:`compute` so the package
keeps no import-time dask dependency (the Phase 06 multiprocessing shim
shares the substrate). Both graph dialects execute:

- **Legacy tuple tasks** (``(func, arg, ...)``) ship as a self-contained
  recursive evaluator plus resolved dependency values, so workers never need
  dask installed.
- **Modern task-spec nodes** (``dask._task_spec``, dask >= 2024.12 — what
  ``dask.delayed``/``dask.bag`` emit today) are callable with a mapping of
  dependency values and pickle by reference to dask; they execute on workers
  because package mirroring ships the driver's dask there.

Literal graph values, key aliases, and ``Alias``/``DataNode`` nodes resolve
locally without a worker round-trip.
"""

from __future__ import annotations

import asyncio
from typing import Any

from . import _bridge

__all__ = ["compute", "get"]


def _evaluate_node(node: Any, values: dict[Any, Any]) -> Any:
    """Recursively evaluate a legacy-dialect graph node against resolved
    dependency ``values`` (the dask graph spec: tasks are tuples with a
    callable head, lists are traversed, keys substitute, the rest are
    literals).

    Runs on workers inside cloudpickle payloads — the package is registered
    pickle-by-value, so this function travels with each task. It must stay
    self-contained: no dask, no imports, no references to other globals.
    """
    if isinstance(node, list):
        return [_evaluate_node(item, values) for item in node]
    if type(node) is tuple and node and callable(node[0]):
        return node[0](*(_evaluate_node(arg, values) for arg in node[1:]))
    try:
        if node in values:
            return values[node]
    except TypeError:  # unhashable literal (dict, set, ...)
        pass
    return node


def _nested_get(keys: Any, cache: dict[Any, Any]) -> Any:
    """Assemble results following the nesting of ``keys``, like dask's own
    ``get``: nested key lists become tuples of results."""
    if isinstance(keys, list):
        return tuple(_nested_get(k, cache) for k in keys)
    return cache[keys]


async def get(dsk: Any, keys: Any, pool: Any = None, **kwargs: Any) -> Any:
    """Execute a dask graph on the worker pool; return the values of ``keys``.

    Parameters
    ----------
    dsk:
        A dask graph — a mapping of key to node, or anything with a
        ``__dask_graph__()`` (HighLevelGraph, expression) to materialize one.
    keys:
        A key or arbitrarily nested lists of keys, like dask's own ``get``;
        the result mirrors the nesting (lists come back as tuples).
    pool:
        A :class:`~pyodide_pool._bridge.WorkerPool` to dispatch through;
        defaults to the pool registered as ``js_pyodide_pool.pool``.
    kwargs:
        Accepted for scheduler-call compatibility (``num_workers`` etc.) and
        ignored: the JS pool bounds concurrency, the scheduler dispatches
        everything that is ready.
    """
    from dask.core import flatten, get_dependencies, istask, reverse_dict, toposort

    try:
        from dask._task_spec import Alias, DataNode, GraphNode
    except ImportError:  # dask predates the task-spec dialect
        Alias = DataNode = GraphNode = ()  # type: ignore[assignment]

    if not isinstance(dsk, dict) and hasattr(dsk, "__dask_graph__"):
        dsk = dsk.__dask_graph__()
    dsk = dict(dsk)

    requested = list(flatten(keys)) if isinstance(keys, list) else [keys]
    missing = [k for k in requested if k not in dsk]
    if missing:
        raise KeyError(f"requested keys not in the graph: {missing!r}")

    dependencies = {k: get_dependencies(dsk, k) for k in dsk}
    toposort(dsk, dependencies=dependencies)  # raises on cycles
    dependents = reverse_dict(dependencies)
    indegree = {k: len(deps) for k, deps in dependencies.items()}

    submit = _bridge.submit if pool is None else pool.submit
    cache: dict[Any, Any] = {}
    pending: dict[Any, asyncio.Future[Any]] = {}
    ready = [k for k in dsk if indegree[k] == 0]

    def finish(key: Any) -> None:
        for dependent in dependents.get(key, ()):
            indegree[dependent] -= 1
            if indegree[dependent] == 0:
                ready.append(dependent)

    def dispatch_ready() -> None:
        # Drain the ready list: local nodes resolve immediately (readying
        # further keys, hence the loop), remote tasks become pending futures.
        while ready:
            key = ready.pop()
            node = dsk[key]
            values = {dep: cache[dep] for dep in dependencies[key]}
            if isinstance(node, GraphNode):
                if isinstance(node, Alias) or isinstance(node, DataNode):
                    cache[key] = node(values)  # cheap: no worker round-trip
                    finish(key)
                else:
                    pending[key] = asyncio.ensure_future(submit(node, values))
            elif istask(node):
                pending[key] = asyncio.ensure_future(
                    submit(_evaluate_node, node, values)
                )
            else:
                # Literal, key alias, or container of keys/literals.
                cache[key] = _evaluate_node(node, values)
                finish(key)

    try:
        dispatch_ready()
        while pending:
            done, _ = await asyncio.wait(
                pending.values(), return_when=asyncio.FIRST_COMPLETED
            )
            for key in [k for k, fut in pending.items() if fut in done]:
                cache[key] = pending.pop(key).result()
                finish(key)
            dispatch_ready()
    except BaseException:
        # Fail fast: drop the outstanding work and let the first error
        # (already carrying its remote traceback, see _bridge) propagate.
        if pending:
            for fut in pending.values():
                fut.cancel()
            await asyncio.gather(*pending.values(), return_exceptions=True)
        raise

    return _nested_get(keys, cache)


async def compute(
    *args: Any,
    traverse: bool = True,
    optimize_graph: bool = True,
    pool: Any = None,
    **kwargs: Any,
) -> Any:
    """Compute dask collections on the worker pool.

    The async counterpart of :func:`dask.compute`: accepts ``dask.delayed``
    objects, bags, arrays, plain values, and (with ``traverse=True``)
    builtin containers of any of these. A single argument returns its bare
    result — ``await compute(delayed_obj)`` matches ``delayed_obj.compute()``
    — while multiple arguments return a tuple like ``dask.compute``.

    On expression-based dask (2025+) this follows ``dask.base.compute``:
    ``collections_to_expr`` + ``FinalizeCompute`` compile each collection's
    finalization into the graph itself. On older dask it falls back to the
    classic ``collections_to_dsk`` + ``__dask_postcompute__`` protocol.
    """
    from dask.base import unpack_collections
    from dask.core import flatten

    collections, repack = unpack_collections(*args, traverse=traverse)
    if not collections:
        return args[0] if len(args) == 1 else args

    try:
        from dask._expr import FinalizeCompute
        from dask.base import collections_to_expr
    except ImportError:  # pre-expression dask: classic collection protocol
        from dask.base import collections_to_dsk

        dsk = collections_to_dsk(collections, optimize_graph, **kwargs)
        nested_keys = [c.__dask_keys__() for c in collections]
        postcomputes = [c.__dask_postcompute__() for c in collections]
        results = await get(dsk, nested_keys, pool=pool)
        out = repack(
            [f(r, *extra) for r, (f, extra) in zip(results, postcomputes)]
        )
    else:
        expr = FinalizeCompute(collections_to_expr(collections, optimize_graph))
        expr = expr.optimize()
        keys = list(flatten(expr.__dask_keys__()))
        results = await get(expr.__dask_graph__(), keys, pool=pool)
        out = repack(results)

    return out[0] if len(args) == 1 else out
