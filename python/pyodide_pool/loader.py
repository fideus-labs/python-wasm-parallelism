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

Environment requirements: nested-worker support (all Chromium-based
browsers; Firefox and Safari in current versions) and network access to
the Pyodide CDN. The pool is postMessage-based, so it does NOT need a
cross-origin-isolated page — it runs on plain static hosts like GitHub
Pages (verified by the Pages deployment of demos/jupyterlite). COOP/COEP
headers (scripts/serve-lite.mjs sends them) only add SharedArrayBuffer
extras such as the JupyterLite kernel's coincident transport.
"""

from __future__ import annotations

from . import _bridge
from ._bridge import WorkerPool

__all__ = ["DEFAULT_JS_URL", "create_pool"]

#: Where the JupyterLite site serves the bundle, as a path RELATIVE to the
#: deployment's base URL (``files/`` holds the site contents). It is resolved
#: against the base URL derived from the kernel worker's ``location`` (see
#: :data:`_IMPORT_JS`), so the default works whether the site is hosted at the
#: origin root (scripts/serve-lite.mjs) or under a sub-path such as GitHub
#: Pages (``/<repo>/``) — no explicit ``js_url`` needed either way.
DEFAULT_JS_URL = "files/assets/pyodide-pool.browser.js"

# JS that dynamically ``import()``s the browser bundle. Two wrinkles make a
# plain ``import(url)`` wrong inside JupyterLite's Pyodide kernel:
#   * ``import()`` resolves relative/path-absolute URLs against the REFERRER
#     module — here pyodide.asm.js, loaded from the jsDelivr CDN — so a
#     site-relative path would 404 on the CDN rather than the site.
#   * A path-absolute ``/files/...`` resolved against the page origin drops
#     any deployment sub-path, so GitHub Pages (``/<repo>/``) 404s on
#     ``origin/files/...`` — the original symptom this bootstrap fixes.
# The kernel worker is served from ``<baseUrl>extensions/.../<name>.worker.js``
# (jupyterlite-pyodide-kernel's ``initWorker``), so everything up to the slash
# before ``extensions/`` is the deployment base URL. Resolve the base-relative
# bundle URL against that, falling back to the origin root. A fully-qualified
# URL (http:, https:, file:, blob:, data:) and Node (no global ``location``)
# are imported as given.
_IMPORT_JS = r"""
(async (u) => {
  if (typeof location === 'undefined') return import(u);
  if (/^[a-z][a-z0-9+.\-]*:/i.test(u)) return import(u);
  const here = location.href;
  const rel = u.replace(/^\/+/, '');
  const ext = here.indexOf('/extensions/');
  const base = ext === -1 ? new URL('/', here).href : here.slice(0, ext + 1);
  const candidates = [...new Set([new URL(rel, base).href, new URL('/' + rel, here).href])];
  let lastErr;
  for (const c of candidates) {
    try { return await import(c); } catch (e) { lastErr = e; }
  }
  throw lastErr;
})
""".strip()


async def create_pool(pool_size: int = 4, js_url: str | None = None) -> WorkerPool:
    """Import the JS bundle, start a worker pool, and make it the default.

    Parameters
    ----------
    pool_size:
        Maximum number of concurrent workers, one Pyodide interpreter each.
    js_url:
        URL of the self-contained browser bundle, defaulting to
        :data:`DEFAULT_JS_URL`. A base-relative path (``files/...``) is
        resolved against the JupyterLite deployment base URL; a full URL
        (``http(s)://``, or a ``file://`` URL under Node) is used as-is.
    """
    from js import Object
    from pyodide.code import run_js
    from pyodide.ffi import to_js

    url = DEFAULT_JS_URL if js_url is None else js_url
    # Resolve and import the bundle against the deployment base URL; see
    # _IMPORT_JS for why a plain import() / path-absolute URL is wrong here.
    # repr(url) yields a JS-safe string literal (matching the old f-string's
    # {url!r}); build the call by concatenation so the JS body's braces are
    # not read as Python format fields.
    module = await run_js(_IMPORT_JS + "(" + repr(url) + ")")
    options = to_js({"poolSize": pool_size}, dict_converter=Object.fromEntries)
    pool = WorkerPool(module.createPool(options))
    _bridge.set_default_pool(pool)
    return pool
