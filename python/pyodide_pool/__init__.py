"""Run Python callables and dask graphs on a pool of Pyodide Web Workers.

This package runs in the MAIN Pyodide instance (the driver). The embedding
JS creates a ``PyodidePool`` and registers it before use::

    pyodide.registerJsModule('js_pyodide_pool', { pool })

Inside a Pyodide kernel with no JS embedder (JupyterLite notebooks), let
``loader`` pull in the self-contained JS bundle instead::

    pool = await pyodide_pool.create_pool(pool_size=4)

Driver usage::

    import pyodide_pool

    value = await pyodide_pool.submit(fn, *args, **kwargs)  # one remote call
    result = await pyodide_pool.compute(delayed_obj)        # dask collections
    values = await pyodide_pool.get(dsk, keys)              # raw dask graph

Design: docs/architecture/dask-scheduler-design.md. The package is pure
Python with no hard dask dependency — ``compute``/``get`` import dask lazily
via ``scheduler`` — so it can also back the Phase 06 multiprocessing shim.
"""

from __future__ import annotations

import sys
from typing import Any

import cloudpickle

from ._bridge import (
    RemoteExecutionError,
    RemoteTraceback,
    WorkerPool,
    default_pool,
    set_default_pool,
    submit,
)
from ._packages import EXCLUDED_FROM_MIRROR, PackageSnapshot, snapshot_packages

__version__ = "0.1.1"

__all__ = [
    "EXCLUDED_FROM_MIRROR",
    "PackageSnapshot",
    "RemoteExecutionError",
    "RemoteTraceback",
    "WorkerPool",
    "compute",
    "create_pool",
    "default_pool",
    "get",
    "set_default_pool",
    "snapshot_packages",
    "submit",
]

# Helpers from this package that end up inside task payloads (the
# scheduler's nested-expression evaluator, user wrappers) must travel BY
# VALUE so workers never need pyodide_pool installed. Registering the
# package covers all submodules — cloudpickle walks parent packages.
cloudpickle.register_pickle_by_value(sys.modules[__name__])


def __getattr__(name: str) -> Any:
    # compute/get live in .scheduler, resolved lazily: scheduler pulls in
    # dask, which must not become an import-time dependency of the package.
    if name in ("compute", "get"):
        from . import scheduler

        value = getattr(scheduler, name)
        globals()[name] = value
        return value
    # create_pool lives in .loader, resolved lazily for symmetry: it is only
    # meaningful where the JS bundle is reachable (browser kernels, Node).
    if name == "create_pool":
        from .loader import create_pool

        globals()[name] = create_pool
        return create_pool
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
