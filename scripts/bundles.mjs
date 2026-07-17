/**
 * Shared esbuild bundle definitions for the library artifacts.
 *
 * Used by scripts/build.mjs (`npm run build`), scripts/build-lite-assets.mjs
 * (the JupyterLite payload) and the vitest suites (tests/helpers.ts), so
 * every consumer builds the exact same artifacts:
 *
 *   1. src/worker/pyodide-worker.ts -> dist/pyodide-worker.js
 *      The Web Worker entry. Bundled as ESM, platform-neutral so the same
 *      bundle loads in browsers and Node worker_threads. `pyodide` stays
 *      external: in Node it must resolve from node_modules so loadPyodide()
 *      can find the bundled WASM assets on disk; in browsers it is served
 *      separately (CDN or static assets).
 *   2. src/index.ts -> dist/index.js
 *      The library entry (PyodidePool public API). Runtime dependencies stay
 *      external — consumers install them.
 *   3. src/browser.ts -> dist/pyodide-pool.browser.js
 *      ONE self-contained ESM file: the public API with worker-pool inlined
 *      AND the worker bundle embedded as a string (spawned via Blob URL), so
 *      it can be import()ed from contexts with no bundler — JupyterLite's
 *      Pyodide kernel worker. Only dynamic-import externals remain
 *      (`pyodide`, `web-worker`), neither of which loads in a browser.
 */
import { createRequire } from 'node:module'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { build } from 'esbuild'

export const rootDir = fileURLToPath(new URL('..', import.meta.url))

const shared = {
  bundle: true,
  format: 'esm',
  platform: 'neutral',
  target: 'es2022',
  logLevel: 'silent',
}

/** Bundle the worker entry; `options` merge over the canonical settings. */
export function buildWorkerBundle(options = {}) {
  return build({
    ...shared,
    entryPoints: [path.join(rootDir, 'src', 'worker', 'pyodide-worker.ts')],
    outfile: path.join(rootDir, 'dist', 'pyodide-worker.js'),
    external: ['pyodide'],
    ...options,
  })
}

/** Bundle the library entry; `options` merge over the canonical settings. */
export function buildIndexBundle(options = {}) {
  return build({
    ...shared,
    entryPoints: [path.join(rootDir, 'src', 'index.ts')],
    outfile: path.join(rootDir, 'dist', 'index.js'),
    external: ['pyodide', '@fideus-labs/worker-pool', 'web-worker'],
    ...options,
  })
}

/**
 * Bundle the self-contained browser file. The worker is first bundled
 * in-memory (no sourcemap — its text is embedded verbatim) and injected via
 * define, together with the installed pyodide version that pins the CDN
 * default. Resolves with the absolute outfile path and that version.
 */
export async function buildBrowserBundle(options = {}) {
  const worker = await buildWorkerBundle({ write: false, sourcemap: false, logLevel: 'silent' })
  const workerSource = worker.outputFiles[0].text
  const pyodideVersion = createRequire(import.meta.url)('pyodide/package.json').version
  const outfile = path.join(rootDir, 'dist', 'pyodide-pool.browser.js')
  await build({
    ...shared,
    platform: 'browser',
    entryPoints: [path.join(rootDir, 'src', 'browser.ts')],
    outfile,
    external: ['pyodide', 'web-worker'],
    define: {
      __PYODIDE_WORKER_SOURCE__: JSON.stringify(workerSource),
      __PYODIDE_VERSION__: JSON.stringify(pyodideVersion),
    },
    ...options,
  })
  return { outfile, pyodideVersion }
}
