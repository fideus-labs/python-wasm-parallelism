"""Package-mirroring snapshot helpers (driver side).

Workers are stateless and must satisfy the imports of whatever user code
they receive. The driver therefore snapshots what its own Pyodide instance
has installed and rides the snapshot on every submitted task
(``_bridge.WorkerPool.submit``); the worker's module-scope installed-set
makes replaying the same snapshot cheap and idempotent, so no separate
driver/worker sync protocol exists.
"""

from __future__ import annotations

import re
from typing import NamedTuple

#: Never mirrored to workers: ``pyodide_pool`` travels by value inside task
#: payloads (it is never installed anywhere, see ``__init__``), the worker's
#: execPickled shim loads cloudpickle itself, and micropip is the install
#: mechanism, not a task dependency. The second group is JupyterLite kernel
#: machinery — never a task dependency, and piplite installs it from its
#: bundled index while reporting ``source == "pypi"``, so name-based
#: mirroring would resolve against REAL PyPI where ``pyodide-kernel`` does
#: not even exist (and ``ipykernel`` pulls ``tornado``, which has no pure
#: wheel there).
EXCLUDED_FROM_MIRROR = frozenset(
    {"pyodide_pool", "cloudpickle", "micropip"}
    | {"piplite", "pyodide_kernel", "ipykernel", "comm", "widgetsnbextension"}
)

#: ``pyodide.loadedPackages`` value for packages from the distribution.
_DEFAULT_CHANNEL = "default channel"


class PackageSnapshot(NamedTuple):
    """What the driver has installed, split by worker install mechanism."""

    packages: list[str]
    """Pyodide-distribution names, installed via ``pyodide.loadPackage``."""

    wheels: list[str]
    """micropip targets: PyPI names or wheel URLs."""


def _is_url(value: str) -> bool:
    return "://" in value or value.endswith(".whl")


def _canonical(name: str) -> str:
    """PEP 503 name normalization — micropip lists ``fake_pkg`` as
    ``fake-pkg``, so exclusion and dedup must compare canonical forms."""
    return re.sub(r"[-_.]+", "-", name).lower()


_EXCLUDED_CANONICAL = frozenset(map(_canonical, EXCLUDED_FROM_MIRROR))


def snapshot_packages() -> PackageSnapshot:
    """Snapshot the driver's installed packages for mirroring to workers.

    Combines the JS side's ``pyodide.loadedPackages`` (covers both
    ``pyodide.loadPackage`` and micropip installs; values are
    ``"default channel"`` for distribution packages, otherwise the install
    source) with ``micropip.list()`` when micropip is present. Outside a
    Pyodide driver both sources are absent and the snapshot is empty.
    """
    packages: dict[str, str] = {}  # canonical -> name as first seen
    wheels: set[str] = set()

    def add(name: str, source: str) -> None:
        canonical = _canonical(name)
        if canonical in _EXCLUDED_CANONICAL:
            return
        if source in (_DEFAULT_CHANNEL, "pyodide"):
            packages.setdefault(canonical, name)
        elif _is_url(source):
            wheels.add(source)
        elif canonical not in packages:
            # Unknown source marker (e.g. "pypi"): let the worker's micropip
            # resolve the package by name.
            wheels.add(name)

    try:
        import pyodide_js
    except ImportError:
        pyodide_js = None
    if pyodide_js is not None:
        for name, channel in pyodide_js.loadedPackages.to_py().items():
            add(name, channel)

    try:
        import micropip
    except ImportError:
        micropip = None
    if micropip is not None:
        for name, meta in micropip.list().items():
            add(name, str(getattr(meta, "source", "") or ""))

    return PackageSnapshot(sorted(packages.values()), sorted(wheels))
