"""The ``multiprocessing.Pool`` shim over ``pyodide_pool``.

Every remote execution goes through ``pyodide_pool`` — batched chunks as
one JS ``mapPickled`` run (``WorkerPool.start_batch``), streamed chunks as
single ``submit`` calls — so cloudpickle/JS bridging and remote-exception
re-raising (original type with ``RemoteTraceback`` chained) live in exactly
one place.

The async-native core is the portable surface: ``amap`` / ``astarmap`` /
``aapply`` / ``imap`` / ``imap_unordered`` / ``ajoin`` plus
``AsyncResult.aget`` / ``await_ready``. The stdlib-shaped sync methods are
thin wrappers over it via ``_block_on``: JSPI ``run_sync`` where the
runtime supports stack switching, a ``RuntimeError`` naming the exact
async replacement everywhere else.

Design + API mapping: docs/architecture/multiprocessing-shim-design.md.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Coroutine, Iterable, Mapping, Sequence
from typing import Any

import pyodide_pool
from pyodide_pool._bridge import BatchRun, Call

try:  # the class users already catch; importable on Emscripten
    from multiprocessing import TimeoutError
except Exception:  # pragma: no cover — stdlib multiprocessing unavailable

    class TimeoutError(Exception):  # type: ignore[no-redef]
        """Raised when a blocking ``AsyncResult`` call times out."""


__all__ = ["AsyncResult", "Pool", "TimeoutError", "cpu_count"]

# Pool lifecycle states (stdlib names).
_RUN, _CLOSE, _TERMINATE = "RUN", "CLOSE", "TERMINATE"

_JSPI_HINT = (
    "Synchronous blocking needs WebAssembly JSPI stack switching "
    "(pyodide.ffi.run_sync): on by default in Chrome 137+, in Node 24 via "
    "--experimental-wasm-jspi, behind a flag in Firefox, not yet in Safari."
)


def _block_on(awaitable: Coroutine[Any, Any, Any], replacement: str) -> Any:
    """Run ``awaitable`` to completion synchronously where the runtime can,
    else raise the guidance error naming the exact async replacement.

    Capability is detected PER CALL, not at import: ``can_run_sync()``
    depends on how Python was entered (async entry via ``runPythonAsync`` /
    ``callPromising`` allows stack switching; plain ``runPython`` does not).
    """
    try:
        from pyodide.ffi import can_run_sync, run_sync

        capable = can_run_sync()
    except ImportError:  # non-Pyodide Python (CI helpers, docs builds)
        capable = False
    if capable:
        return run_sync(awaitable)
    awaitable.close()  # don't leak a never-awaited coroutine
    raise RuntimeError(
        f"this runtime cannot block on pool results — use the async form: "
        f"{replacement}\n{_JSPI_HINT}"
    )


def cpu_count() -> int:
    """``navigator.hardwareConcurrency`` (browsers, workers, Node >= 21);
    1 where unavailable."""
    try:
        import js

        count = int(js.navigator.hardwareConcurrency)
        if count >= 1:
            return count
    except (ImportError, AttributeError, TypeError, ValueError):
        pass
    return 1


def _run_chunk(func: Callable[..., Any], items: list[Any]) -> list[Any]:
    """Executes ON WORKERS (ships by value): one ``map`` chunk."""
    return [func(item) for item in items]


def _run_star_chunk(func: Callable[..., Any], items: list[Any]) -> list[Any]:
    """Executes ON WORKERS (ships by value): one ``starmap`` chunk."""
    return [func(*item) for item in items]


def _default_chunksize(n_items: int, processes: int) -> int:
    """CPython's own ``Pool._map_async`` heuristic: ~4 chunks per (soft-cap)
    worker, so per-message overhead amortizes over coarse batches."""
    chunksize, extra = divmod(n_items, processes * 4)
    if extra:
        chunksize += 1
    return max(chunksize, 1)


def _split(items: list[Any], chunksize: int) -> list[list[Any]]:
    return [items[i : i + chunksize] for i in range(0, len(items), chunksize)]


class AsyncResult:
    """Result handle of the ``*_async`` methods, backed by an asyncio task.

    ``ready``/``successful`` are non-blocking and stdlib-shaped. The
    portable completion forms are awaitables — ``await result`` (sugar for
    ``aget()``) and ``await_ready()``; the blocking ``get``/``wait`` work
    only under JSPI (see ``_block_on``).
    """

    def __init__(
        self,
        task: asyncio.Task[Any],
        callback: Callable[[Any], object] | None = None,
        error_callback: Callable[[BaseException], object] | None = None,
    ) -> None:
        self._task = task
        self._callback = callback
        self._error_callback = error_callback
        task.add_done_callback(self._fire_callbacks)

    def _fire_callbacks(self, task: asyncio.Task[Any]) -> None:
        if task.cancelled():
            return
        # .exception() also marks the failure as retrieved, so an abandoned
        # failed result never trips asyncio's never-retrieved warning.
        exc = task.exception()
        if exc is None:
            if self._callback is not None:
                self._callback(task.result())
        elif self._error_callback is not None:
            self._error_callback(exc)

    def ready(self) -> bool:
        return self._task.done()

    def successful(self) -> bool:
        if not self._task.done():
            raise ValueError(f"{self!r} not ready")
        return not self._task.cancelled() and self._task.exception() is None

    async def aget(self, timeout: float | None = None) -> Any:
        """Await the result; a failed task re-raises its original exception
        (worker traceback chained as ``RemoteTraceback``). On ``timeout``
        raises :class:`TimeoutError` and leaves the computation running —
        a later ``aget()`` can still collect it (stdlib ``get`` semantics,
        hence the ``shield``)."""
        if timeout is None:
            return await asyncio.shield(self._task)
        try:
            return await asyncio.wait_for(asyncio.shield(self._task), timeout)
        except asyncio.TimeoutError:
            raise TimeoutError from None

    async def await_ready(self, timeout: float | None = None) -> None:
        """Awaitable form of ``wait``: returns once ready or after
        ``timeout`` seconds, never raising on timeout (stdlib ``wait``)."""
        await asyncio.wait({self._task}, timeout=timeout)

    def get(self, timeout: float | None = None) -> Any:
        return _block_on(self.aget(timeout), "await result.aget(timeout)")

    def wait(self, timeout: float | None = None) -> None:
        return _block_on(
            self.await_ready(timeout), "await result.await_ready(timeout)"
        )

    def __await__(self) -> Any:
        return self.aget().__await__()


class Pool:
    """``multiprocessing.Pool`` over the shared Pyodide worker pool.

    The JS pool's ``poolSize`` is the HARD concurrency cap; ``processes``
    is a driver-side soft cap (``asyncio.Semaphore``) on in-flight chunks,
    which is what makes ``Pool(1)``/``Pool(2)``/``Pool(4)`` behave
    differently on one shared 4-worker JS pool.
    """

    def __init__(
        self,
        processes: int | None = None,
        initializer: Callable[..., object] | None = None,
        initargs: Sequence[Any] = (),
        maxtasksperchild: int | None = None,
        context: Any = None,
        *,
        pool: pyodide_pool.WorkerPool | None = None,
    ) -> None:
        if initializer is not None:
            raise NotImplementedError(
                "Pool(initializer=...) is not supported yet (planned as a "
                "cloudpickled per-worker prelude); ship state inside func "
                "instead. See docs/architecture/multiprocessing-shim-design.md."
            )
        if maxtasksperchild is not None or context is not None:
            raise NotImplementedError(
                "Pool(maxtasksperchild=/context=) has no meaning here — "
                "worker recycling is the JS pool's policy. See "
                "docs/architecture/multiprocessing-shim-design.md."
            )
        if processes is None:
            processes = cpu_count()
        if not isinstance(processes, int) or processes < 1:
            raise ValueError("Number of processes must be at least 1")
        self._processes = processes
        self._pool = pool if pool is not None else pyodide_pool.default_pool()
        self._semaphore = asyncio.Semaphore(processes)
        self._state = _RUN
        self._active_batches: set[BatchRun] = set()
        self._pending: set[asyncio.Task[Any]] = set()

    def __repr__(self) -> str:
        return f"<wasm_multiprocessing.Pool processes={self._processes} state={self._state}>"

    # -- internals ----------------------------------------------------------

    def _check_running(self) -> None:
        if self._state != _RUN:
            raise ValueError("Pool not running")

    def _spawn(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """Create a tracked asyncio task (``join`` awaits the tracked set;
        ``terminate`` cancels it). Works from sync driver code too:
        Pyodide's WebLoop accepts task creation outside a coroutine."""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.get_event_loop()
        task = loop.create_task(coro)
        self._pending.add(task)
        task.add_done_callback(self._forget)
        return task

    def _forget(self, task: asyncio.Task[Any]) -> None:
        self._pending.discard(task)
        if not task.cancelled():
            task.exception()  # abandoned failures stay quiet (see AsyncResult)

    def _map_calls(
        self,
        runner: Callable[..., Any],
        func: Callable[..., Any],
        iterable: Iterable[Any],
        chunksize: int | None,
    ) -> list[Call]:
        """Chunk ``iterable`` into ``(runner, (func, chunk), {})`` call
        triples — one chunk = one task message. Consumed eagerly (v1)."""
        items = list(iterable)
        if chunksize is None:
            chunksize = _default_chunksize(len(items), self._processes) if items else 1
        elif chunksize < 1:
            raise ValueError("Chunksize must be 1+")
        return [(runner, (func, chunk), {}) for chunk in _split(items, chunksize)]

    async def _submit_call(self, call: Call) -> Any:
        """One chunk as a single ``submit``, gated by the soft cap."""
        func, args, kwargs = call
        async with self._semaphore:
            return await self._pool.submit(func, *args, **kwargs)

    async def _run_chunks(self, calls: list[Call]) -> list[Any]:
        """Chunk results in input order.

        Fast path: when the soft cap cannot bind tighter than the JS pool's
        own worker cap, the whole call is ONE ``mapPickled`` run (single
        FIFO queue, one cancel handle). Otherwise chunks go through
        semaphore-gated single submits so at most ``processes`` are ever in
        flight.
        """
        if self._processes >= self._pool.pool_size:
            batch = self._pool.start_batch(calls)
            self._active_batches.add(batch)
            try:
                return await batch.results()
            finally:
                self._active_batches.discard(batch)
        return list(await asyncio.gather(*(self._submit_call(call) for call in calls)))

    async def _gather_map(self, calls: list[Call]) -> list[Any]:
        if not calls:
            return []
        chunk_results = await self._run_chunks(calls)
        return [result for chunk in chunk_results for result in chunk]

    def _stream_chunks(
        self,
        runner: Callable[..., Any],
        func: Callable[..., Any],
        iterable: Iterable[Any],
        chunksize: int,
    ) -> list[asyncio.Task[Any]]:
        """Per-chunk submit futures for ``imap*`` — a ``mapPickled`` run
        settles only as a whole, so streaming needs one future per chunk."""
        if chunksize < 1:
            raise ValueError("Chunksize must be 1+")
        calls = self._map_calls(runner, func, iterable, chunksize)
        return [self._spawn(self._submit_call(call)) for call in calls]

    # -- async-native core --------------------------------------------------

    async def amap(
        self,
        func: Callable[[Any], Any],
        iterable: Iterable[Any],
        chunksize: int | None = None,
    ) -> list[Any]:
        """Async ``map``: results in input order."""
        self._check_running()
        return await self._spawn(self._gather_map(self._map_calls(_run_chunk, func, iterable, chunksize)))

    async def astarmap(
        self,
        func: Callable[..., Any],
        iterable: Iterable[Sequence[Any]],
        chunksize: int | None = None,
    ) -> list[Any]:
        """Async ``starmap``: each item is an argument tuple."""
        self._check_running()
        return await self._spawn(self._gather_map(self._map_calls(_run_star_chunk, func, iterable, chunksize)))

    async def aapply(
        self,
        func: Callable[..., Any],
        args: Sequence[Any] = (),
        kwds: Mapping[str, Any] | None = None,
    ) -> Any:
        """Async ``apply``: one unbatched call."""
        self._check_running()
        return await self._spawn(self._pool.submit(func, *args, **dict(kwds or {})))

    def imap(
        self,
        func: Callable[[Any], Any],
        iterable: Iterable[Any],
        chunksize: int = 1,
    ) -> AsyncIterator[Any]:
        """Async generator over results IN INPUT ORDER, yielding as chunks
        complete. Chunks dispatch eagerly at call time (v1), bounded in
        flight by the soft cap."""
        self._check_running()
        futures = self._stream_chunks(_run_chunk, func, iterable, chunksize)

        async def iterate() -> AsyncIterator[Any]:
            for future in futures:
                for result in await future:
                    yield result

        return iterate()

    def imap_unordered(
        self,
        func: Callable[[Any], Any],
        iterable: Iterable[Any],
        chunksize: int = 1,
    ) -> AsyncIterator[Any]:
        """Like ``imap`` but yields whole chunks in COMPLETION order."""
        self._check_running()
        futures = self._stream_chunks(_run_chunk, func, iterable, chunksize)

        async def iterate() -> AsyncIterator[Any]:
            for next_done in asyncio.as_completed(futures):
                for result in await next_done:
                    yield result

        return iterate()

    async def ajoin(self) -> None:
        """Await every outstanding result. Stdlib rule kept: ``ValueError``
        unless the pool was closed/terminated first."""
        if self._state == _RUN:
            raise ValueError("Pool is still running")
        while self._pending:
            await asyncio.wait(set(self._pending))

    # -- stdlib-shaped API --------------------------------------------------

    def map(
        self,
        func: Callable[[Any], Any],
        iterable: Iterable[Any],
        chunksize: int | None = None,
    ) -> list[Any]:
        self._check_running()
        return _block_on(self.amap(func, iterable, chunksize), "await pool.amap(func, iterable)")

    def starmap(
        self,
        func: Callable[..., Any],
        iterable: Iterable[Sequence[Any]],
        chunksize: int | None = None,
    ) -> list[Any]:
        self._check_running()
        return _block_on(self.astarmap(func, iterable, chunksize), "await pool.astarmap(func, iterable)")

    def apply(
        self,
        func: Callable[..., Any],
        args: Sequence[Any] = (),
        kwds: Mapping[str, Any] | None = None,
    ) -> Any:
        self._check_running()
        return _block_on(self.aapply(func, args, kwds), "await pool.aapply(func, args, kwds)")

    def map_async(
        self,
        func: Callable[[Any], Any],
        iterable: Iterable[Any],
        chunksize: int | None = None,
        callback: Callable[[Any], object] | None = None,
        error_callback: Callable[[BaseException], object] | None = None,
    ) -> AsyncResult:
        self._check_running()
        calls = self._map_calls(_run_chunk, func, iterable, chunksize)
        return AsyncResult(self._spawn(self._gather_map(calls)), callback, error_callback)

    def starmap_async(
        self,
        func: Callable[..., Any],
        iterable: Iterable[Sequence[Any]],
        chunksize: int | None = None,
        callback: Callable[[Any], object] | None = None,
        error_callback: Callable[[BaseException], object] | None = None,
    ) -> AsyncResult:
        self._check_running()
        calls = self._map_calls(_run_star_chunk, func, iterable, chunksize)
        return AsyncResult(self._spawn(self._gather_map(calls)), callback, error_callback)

    def apply_async(
        self,
        func: Callable[..., Any],
        args: Sequence[Any] = (),
        kwds: Mapping[str, Any] | None = None,
        callback: Callable[[Any], object] | None = None,
        error_callback: Callable[[BaseException], object] | None = None,
    ) -> AsyncResult:
        self._check_running()
        task = self._spawn(self._pool.submit(func, *args, **dict(kwds or {})))
        return AsyncResult(task, callback, error_callback)

    def close(self) -> None:
        """Stop accepting submissions; outstanding work keeps running —
        drain it with ``await pool.ajoin()`` (or ``join()`` under JSPI)."""
        if self._state == _RUN:
            self._state = _CLOSE

    def join(self) -> None:
        if self._state == _RUN:
            raise ValueError("Pool is still running")
        return _block_on(self.ajoin(), "await pool.ajoin()")

    def terminate(self) -> None:
        """Stop without draining, approximately (documented): cancel batched
        chunks not yet started plus every tracked pending future, then
        terminate the JS pool's idle workers. In-flight worker calls run to
        completion — no preemption without an interrupt buffer."""
        self._state = _TERMINATE
        for batch in list(self._active_batches):
            batch.cancel()
        for task in list(self._pending):
            task.cancel()
        self._pool.terminate()

    def __enter__(self) -> Pool:
        self._check_running()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.terminate()

    async def __aenter__(self) -> Pool:
        self._check_running()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.terminate()
