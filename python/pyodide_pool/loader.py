"""Bootstrap the JS worker pool from inside a Pyodide kernel (JupyterLite).

Runs in the MAIN Pyodide instance (the driver), like the rest of the
package. An embedding web app wires the JS side itself (create a
``PyodidePool``, ``pyodide.registerJsModule('js_pyodide_pool', { pool })``),
but notebook code inside JupyterLite's Pyodide kernel has no JS embedder to
lean on — the kernel itself runs in a Web Worker. :func:`create_pool` pulls
the JS side in from Python instead: dynamically ``import()`` the
self-contained browser bundle (``pyodide-pool.browser.js``, built by
``npm run build`` and shipped inside this wheel), construct the JS
``PyodidePool`` — its workers spawn as NESTED Web Workers from a Blob URL
inlined in the bundle — and install the Python :class:`WorkerPool` wrapper
as the default pool, so ``pyodide_pool.submit``/``compute`` work with no
further setup.

The vendored bundle is imported from a ``data:`` URL, so no host has to
serve it; a page with a ``script-src`` CSP must allow ``data:`` (the nested
workers already need ``blob:``).

Environment requirements: nested-worker support (all Chromium-based
browsers; Firefox and Safari in current versions) and network access to
the Pyodide CDN. The pool is postMessage-based, so it does NOT need a
cross-origin-isolated page — it runs on plain static hosts like GitHub
Pages (verified by the Pages deployment of demos/jupyterlite). COOP/COEP
headers (scripts/serve-lite.mjs sends them) only add SharedArrayBuffer
extras such as the JupyterLite kernel's coincident transport.
"""

from __future__ import annotations

from base64 import b64encode
from importlib.resources import files

from . import _bridge
from ._bridge import WorkerPool

__all__ = ["create_pool"]

#: The browser bundle, copied next to this module by scripts/bundles.mjs and
#: listed as a build artifact in pyproject.toml so the wheel carries it.
_BUNDLE = "pyodide-pool.browser.js"


async def create_pool(pool_size: int = 4, js_url: str | None = None) -> WorkerPool:
    """Import the JS bundle, start a worker pool, and make it the default.

    Parameters
    ----------
    pool_size:
        Maximum number of concurrent workers, one Pyodide interpreter each.
    js_url:
        Load the bundle from this URL instead of the vendored copy. Must be
        fully qualified (``https://``, or ``file://`` under Node): inside a
        kernel worker, ``import()`` resolves a relative URL against the CDN
        module evaluating it, not against the site.
    """
    from js import Object
    from pyodide.code import run_js
    from pyodide.ffi import to_js

    if js_url is None:
        # FileNotFoundError here means the wheel was built before the JS
        # bundle (run `npm run build` first).
        source = files(__package__).joinpath(_BUNDLE).read_bytes()
        js_url = "data:text/javascript;base64," + b64encode(source).decode()
    module = await run_js("(url) => import(url)")(js_url)
    options = to_js({"poolSize": pool_size}, dict_converter=Object.fromEntries)
    pool = WorkerPool(module.createPool(options))
    _bridge.set_default_pool(pool)
    return pool
