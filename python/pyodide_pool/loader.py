"""Bootstrap the JS worker pool from inside a Pyodide kernel (JupyterLite).

Runs in the MAIN Pyodide instance (the driver), like the rest of the
package. An embedding web app wires the JS side itself (create a
``PyodidePool``, ``pyodide.registerJsModule('js_pyodide_pool', { pool })``),
but notebook code inside JupyterLite's Pyodide kernel has no JS embedder to
lean on — the kernel itself runs in a Web Worker. :func:`create_pool` pulls
the JS side in from Python instead: dynamically ``import()`` the
self-contained browser bundle (``pyodide-pool.browser.js``, built by
``npm run build`` and copied into the site's ``files/assets/`` by
``npm run build:lite``), construct the JS ``PyodidePool`` — its workers
spawn as NESTED Web Workers from a Blob URL inlined in the bundle — and
install the Python :class:`WorkerPool` wrapper as the default pool, so
``pyodide_pool.submit``/``compute`` work with no further setup.

Environment requirements: a cross-origin-isolated page (COOP/COEP headers,
see scripts/serve-lite.mjs) and nested-worker support (all Chromium-based
browsers; Firefox and Safari in current versions).
"""

from __future__ import annotations

from . import _bridge
from ._bridge import WorkerPool

__all__ = ["DEFAULT_JS_URL", "create_pool"]

#: Where the JupyterLite site serves the bundle when hosted at the domain
#: root (scripts/serve-lite.mjs serves ``_output`` that way). Deployments
#: under a sub-path must pass ``js_url`` explicitly.
DEFAULT_JS_URL = "/files/assets/pyodide-pool.browser.js"


async def create_pool(pool_size: int = 4, js_url: str | None = None) -> WorkerPool:
    """Import the JS bundle, start a worker pool, and make it the default.

    Parameters
    ----------
    pool_size:
        Maximum number of concurrent workers, one Pyodide interpreter each.
    js_url:
        URL of the self-contained browser bundle, defaulting to
        :data:`DEFAULT_JS_URL`. Anything the JS ``import()`` accepts in the
        current context works: an absolute path, a full URL, or a ``file://``
        URL under Node.
    """
    from js import Object
    from pyodide.code import run_js
    from pyodide.ffi import to_js

    url = DEFAULT_JS_URL if js_url is None else js_url
    module = await run_js(f"import({url!r})")
    options = to_js({"poolSize": pool_size}, dict_converter=Object.fromEntries)
    pool = WorkerPool(module.createPool(options))
    _bridge.set_default_pool(pool)
    return pool
