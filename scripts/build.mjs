// esbuild bundling for the Pyodide worker-pool demo — `npm run build`.
//
// The bundle definitions (what is built, what stays external, and why) live
// in scripts/bundles.mjs, shared with the vitest suites and the JupyterLite
// asset build; this script just runs all three with sourcemaps and logging.
import { buildBrowserBundle, buildIndexBundle, buildWorkerBundle } from './bundles.mjs'

await buildWorkerBundle({ sourcemap: true, logLevel: 'info' })
await buildIndexBundle({ sourcemap: true, logLevel: 'info' })
// No sourcemap: the browser bundle is copied into the JupyterLite site as a
// single file, so a sourceMappingURL comment would just 404 there.
await buildBrowserBundle({ logLevel: 'info' })
