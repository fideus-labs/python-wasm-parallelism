/**
 * Entry for the self-contained browser bundle (dist/pyodide-pool.browser.js).
 *
 * Built by scripts/bundles.mjs with the compiled worker code injected as the
 * `__PYODIDE_WORKER_SOURCE__` constant, so the ONE emitted file carries the
 * whole stack: the PyodidePool public API (with @fideus-labs/worker-pool
 * inlined) plus the worker, spawned from a Blob URL. Nothing references a
 * sibling asset or a bare specifier at load time, so the file can be
 * `import()`ed from any context that can spawn workers — in particular from
 * inside JupyterLite's Pyodide kernel worker (see python/pyodide_pool/
 * loader.py), where no bundler resolves asset URLs and the pool's workers
 * are NESTED workers.
 *
 * `pyodide` and `web-worker` stay external as dynamic-only imports: workers
 * boot Pyodide from the moduleURL carried by each request (defaulting to the
 * jsDelivr CDN pinned to the bundled version below), and the web-worker
 * polyfill path is never taken where `Worker` exists globally.
 */
import { PyodidePool } from './pool/pyodide-pool.js'
import type { PyodidePoolOptions } from './pool/pyodide-pool.js'
import type { PyodideSource } from './worker/pyodide-worker.js'

export * from './index.js'

declare const __PYODIDE_WORKER_SOURCE__: string
declare const __PYODIDE_VERSION__: string

/** Pyodide version the CDN default below is pinned to (from package.json). */
export const PYODIDE_VERSION: string = __PYODIDE_VERSION__

const PYODIDE_CDN_BASE = `https://cdn.jsdelivr.net/pyodide/v${__PYODIDE_VERSION__}/full/`

/** Where workers obtain Pyodide unless {@link createPool} is told otherwise. */
export const DEFAULT_PYODIDE_SOURCE: PyodideSource = {
  moduleURL: `${PYODIDE_CDN_BASE}pyodide.mjs`,
  indexURL: PYODIDE_CDN_BASE,
}

let blobUrl: string | null = null

/**
 * Blob URL of the inlined worker bundle, created once per module instance.
 * A blob worker inherits the creating context's origin, so nested
 * `new Worker()` resolves without any served worker asset (and without a
 * COEP-governed fetch for the worker script itself).
 */
export function workerBlobUrl(): string {
  if (blobUrl === null) {
    blobUrl = URL.createObjectURL(new Blob([__PYODIDE_WORKER_SOURCE__], { type: 'text/javascript' }))
  }
  return blobUrl
}

/** Options for {@link createPool}; every field of PyodidePoolOptions, all optional. */
export type CreatePoolOptions = Partial<PyodidePoolOptions>

/**
 * Construct a {@link PyodidePool} wired for the self-contained bundle:
 * workers spawn from the inlined Blob URL and boot Pyodide from the pinned
 * CDN. Both stay overridable (`workerUrl`, `pyodideSource`), and `poolSize`
 * defaults to 4.
 */
export function createPool(options: CreatePoolOptions = {}): PyodidePool {
  return new PyodidePool({
    ...options,
    poolSize: options.poolSize ?? 4,
    workerUrl: options.workerUrl ?? workerBlobUrl(),
    pyodideSource: options.pyodideSource ?? DEFAULT_PYODIDE_SOURCE,
  })
}
