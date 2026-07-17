/**
 * Vite config for the browser demo (web/).
 *
 * COOP/COEP response headers make the served page cross-origin isolated —
 * the precondition for SharedArrayBuffer, which @fideus-labs/worker-pool
 * and Pyodide's pthread support rely on. Both the dev server and `vite
 * preview` (which Playwright runs against the production build) send them.
 *
 * Workers are bundled as ES modules: PyodidePool always constructs
 * `new Worker(url, { type: 'module' })`, and web/main.ts obtains `url` via
 * Vite's `?worker&url` import of src/worker/pyodide-worker.ts, so the same
 * environment-aware worker source that runs under Node worker_threads is
 * served to the browser. `pyodide` stays external to the worker bundle: in
 * the browser the worker imports Pyodide at runtime from the jsDelivr CDN
 * URL carried by each request (see PyodideSource), so the node_modules
 * fallback import must not be resolved at build time.
 */
import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vite'

const crossOriginIsolationHeaders = {
  'Cross-Origin-Opener-Policy': 'same-origin',
  'Cross-Origin-Embedder-Policy': 'require-corp',
}

export default defineConfig({
  root: 'web',
  server: {
    headers: crossOriginIsolationHeaders,
    fs: {
      // web/main.ts imports src/ (pool + worker) and python/ (?raw sources)
      // from outside the Vite root.
      allow: [fileURLToPath(new URL('.', import.meta.url))],
    },
  },
  preview: {
    headers: crossOriginIsolationHeaders,
  },
  worker: {
    format: 'es',
    rollupOptions: {
      external: ['pyodide'],
    },
  },
  optimizeDeps: {
    exclude: ['pyodide'],
  },
  build: {
    target: 'es2022',
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: {
      external: ['pyodide'],
    },
  },
})
