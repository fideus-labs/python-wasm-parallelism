"""``multiprocessing.Pool`` for Pyodide: same API, Web Workers underneath.

Change one import line and existing ``multiprocessing.Pool`` code runs on
the Pyodide worker pool::

    # was: from multiprocessing import Pool
    from wasm_multiprocessing import Pool

    with Pool(4) as pool:
        results = await pool.amap(count_primes, ranges)  # portable everywhere
        results = pool.map(count_primes, ranges)         # blocks only under JSPI

Like ``pyodide_pool``, this package runs ONLY in the driver: the chunk
runners it ships inside task payloads travel by value (pickle-by-value
registration below), so workers never install it.

Sync methods (``map``/``starmap``/``apply``/``join``, ``AsyncResult.get``/
``wait``) truly block only where JSPI stack switching is available
(``pyodide.ffi.can_run_sync()``, detected per call); everywhere else they
raise a ``RuntimeError`` naming the exact async replacement. Anything
fork-shaped (``Process``, ``Queue``, ``Manager``, shared memory, locks)
raises ``NotImplementedError`` — WebAssembly workers share nothing.

Design + full API mapping: docs/architecture/multiprocessing-shim-design.md.
"""

from __future__ import annotations

import sys
from typing import Any

import cloudpickle

from .pool import AsyncResult, Pool, TimeoutError, cpu_count

__version__ = "0.1.0"

__all__ = ["AsyncResult", "Pool", "TimeoutError", "cpu_count"]

# The chunk runners (pool._run_chunk / _run_star_chunk) ship inside
# cloudpickle payloads; registering the package makes them travel BY VALUE
# so workers never need wasm_multiprocessing installed (registering the
# parent package covers submodules — cloudpickle walks parent packages).
cloudpickle.register_pickle_by_value(sys.modules[__name__])

#: stdlib ``multiprocessing`` names that cannot exist on WebAssembly — no
#: processes, no fork, no shared memory, no channels between workers.
#: Attribute access raises immediately instead of failing obscurely
#: mid-task; see the design doc's "out of scope" section.
_UNSUPPORTED = frozenset(
    {
        "Process",
        "Queue",
        "SimpleQueue",
        "JoinableQueue",
        "Pipe",
        "Manager",
        "Value",
        "Array",
        "RawValue",
        "RawArray",
        "Lock",
        "RLock",
        "Semaphore",
        "BoundedSemaphore",
        "Event",
        "Condition",
        "Barrier",
        "shared_memory",
        "current_process",
        "parent_process",
        "active_children",
        "freeze_support",
        "get_context",
        "get_start_method",
        "set_start_method",
        "get_all_start_methods",
        "set_executable",
        "set_forkserver_preload",
        "log_to_stderr",
        "get_logger",
    }
)


def __getattr__(name: str) -> Any:
    if name in _UNSUPPORTED:
        raise NotImplementedError(
            f"wasm_multiprocessing.{name} is intentionally unsupported: "
            "WebAssembly workers share nothing and cannot fork — tasks "
            "communicate only through arguments and results. See "
            "docs/architecture/multiprocessing-shim-design.md (out of scope)."
        )
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
