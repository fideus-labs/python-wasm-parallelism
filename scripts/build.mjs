// esbuild bundling for the Pyodide worker-pool demo.
//
// Two bundles:
//   1. src/worker/pyodide-worker.ts -> dist/pyodide-worker.js
//      The Web Worker entry. Bundled as ESM, platform-neutral so the same
//      bundle loads in browsers and Node worker_threads. `pyodide` stays
//      external: in Node it must resolve from node_modules so loadPyodide()
//      can find the bundled WASM assets on disk; in browsers it is served
//      separately (CDN or static assets).
//   2. src/index.ts -> dist/index.js
//      The library entry (PyodidePool public API). Runtime dependencies stay
//      external — consumers install them.
import { build } from 'esbuild'

const shared = {
  bundle: true,
  format: 'esm',
  platform: 'neutral',
  target: 'es2022',
  sourcemap: true,
  logLevel: 'info',
}

await build({
  ...shared,
  entryPoints: ['src/worker/pyodide-worker.ts'],
  outfile: 'dist/pyodide-worker.js',
  external: ['pyodide'],
})

await build({
  ...shared,
  entryPoints: ['src/index.ts'],
  outfile: 'dist/index.js',
  external: ['pyodide', '@fideus-labs/worker-pool', 'web-worker'],
})
