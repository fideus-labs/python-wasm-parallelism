"""Driver-side bridge to the JS ``PyodidePool``.

This module runs in the MAIN Pyodide instance (the driver), never in
workers: worker-executed code travels inside cloudpickle payloads (the
package is registered pickle-by-value in ``__init__``), so workers never
import this package and it is never installed there.

``submit`` is the single choke point between driver Python and the pool:
cloudpickle the ``(func, args, kwargs)`` call triple, hand the bytes to the
JS pool's ``runPickled`` as a transferable buffer together with the current
package snapshot, await the JS promise (directly awaitable on Pyodide's
event loop), and unpickle the result — or re-raise the remote exception
with its original traceback attached.
"""

from __future__ import annotations

from typing import Any, NoReturn

import cloudpickle
from js import Object, Uint8Array
from pyodide.ffi import JsException, to_js

from ._packages import snapshot_packages

__all__ = [
    "RemoteExecutionError",
    "RemoteTraceback",
    "WorkerPool",
    "default_pool",
    "set_default_pool",
    "submit",
]


class RemoteTraceback(Exception):
    """Formatted traceback of an exception raised on a pool worker.

    Attached as ``__cause__`` when the original exception re-raises on the
    driver (the concurrent.futures pattern), so the user sees where their
    code actually failed, not where the bridge noticed.
    """

    def __init__(self, formatted: str) -> None:
        super().__init__(formatted)
        self.formatted = formatted

    def __str__(self) -> str:
        return f'\n"""\n{self.formatted}"""'


class RemoteExecutionError(Exception):
    """A worker task failed and the original exception could not be
    reconstructed (it was unpicklable); carries the remote traceback text."""

    def __init__(self, message: str, remote_traceback: str | None = None) -> None:
        super().__init__(message)
        self.remote_traceback = remote_traceback


def _resolve_js_pool() -> Any:
    try:
        import js_pyodide_pool  # registered by the embedding JS
    except ImportError as exc:
        raise RuntimeError(
            "No JS worker pool is registered. Create a PyodidePool in JS and "
            "expose it with pyodide.registerJsModule('js_pyodide_pool', "
            "{ pool }) before using pyodide_pool."
        ) from exc
    pool = getattr(js_pyodide_pool, "pool", None)
    if pool is None:
        raise RuntimeError(
            "The js_pyodide_pool module has no 'pool' attribute; register the "
            "JS pool as pyodide.registerJsModule('js_pyodide_pool', { pool })."
        )
    return pool


def _to_js_buffer(data: bytes) -> Any:
    """bytes -> plain JS ArrayBuffer (transferable; runPickled detaches it)."""
    view = Uint8Array.new(len(data))
    view.assign(data)
    return view.buffer


def _to_py_bytes(js_buffer: Any) -> bytes:
    """JS ArrayBuffer (or any view over one) -> bytes."""
    return Uint8Array.new(js_buffer).to_bytes()


def _raise_remote(exc: JsException) -> NoReturn:
    """Re-raise a failed ``runPickled`` as the original Python exception.

    ``JsException`` is itself a JsProxy of the JS error (``PyodideTaskError``),
    so the worker's ``pythonTraceback`` and cloudpickled ``exceptionPayload``
    read straight off the caught exception. Undefined properties surface as
    AttributeError, which ``getattr`` defaults cover.
    """
    formatted = getattr(exc, "pythonTraceback", None)
    payload = getattr(exc, "exceptionPayload", None)
    if payload is not None:
        try:
            remote = cloudpickle.loads(_to_py_bytes(payload))
        except BaseException:
            remote = None
        if isinstance(remote, BaseException):
            if isinstance(formatted, str) and formatted:
                raise remote from RemoteTraceback(formatted)
            raise remote from exc
    message = getattr(exc, "message", None) or str(exc)
    raise RemoteExecutionError(message, remote_traceback=formatted) from exc


class WorkerPool:
    """Thin Python handle over the JS ``PyodidePool``.

    Wraps the pool registered as ``js_pyodide_pool.pool`` by default; pass a
    JsProxy of another pool for explicit wiring (tests, multiple pools).
    """

    def __init__(self, js_pool: Any = None) -> None:
        self._js_pool = js_pool if js_pool is not None else _resolve_js_pool()

    @property
    def pool_size(self) -> int:
        """Maximum number of concurrent workers in the JS pool."""
        return int(self._js_pool.poolSize)

    async def submit(self, func: Any, /, *args: Any, **kwargs: Any) -> Any:
        """Run ``func(*args, **kwargs)`` on a pool worker; return its result.

        The call triple ships as a cloudpickle payload together with the
        driver's current package snapshot, so the worker mirrors the
        driver's installed packages before executing.
        """
        payload = _to_js_buffer(cloudpickle.dumps((func, args, kwargs)))
        packages, wheels = snapshot_packages()
        options = to_js(
            {"packages": packages, "wheels": wheels},
            dict_converter=Object.fromEntries,
        )
        try:
            result = await self._js_pool.runPickled(payload, options)
        except JsException as exc:
            _raise_remote(exc)
        return cloudpickle.loads(_to_py_bytes(result))

    def terminate(self) -> None:
        """Terminate idle workers; the pool stays usable (next task re-boots)."""
        self._js_pool.terminate()


_default_pool: WorkerPool | None = None


def default_pool() -> WorkerPool:
    """The process-wide :class:`WorkerPool` over ``js_pyodide_pool.pool``."""
    global _default_pool
    if _default_pool is None:
        _default_pool = WorkerPool()
    return _default_pool


def set_default_pool(pool: WorkerPool | None) -> None:
    """Install ``pool`` as the process-wide default — the target of
    module-level :func:`submit` and the dask scheduler. ``loader.create_pool``
    calls this so notebooks never wire JS themselves; ``None`` resets to lazy
    resolution via ``js_pyodide_pool``."""
    global _default_pool
    _default_pool = pool


async def submit(func: Any, /, *args: Any, **kwargs: Any) -> Any:
    """Module-level :meth:`WorkerPool.submit` on the default pool."""
    return await default_pool().submit(func, *args, **kwargs)
