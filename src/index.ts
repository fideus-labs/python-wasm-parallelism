/**
 * Public API: parallel CPU-bound Python on a pool of Pyodide Web Workers.
 *
 * `PyodidePool` runs Python snippets across a pool of Web Workers (browser)
 * or worker_threads (Node, via the `web-worker` polyfill), one Pyodide
 * interpreter per worker. Queuing and worker recycling come from
 * @fideus-labs/worker-pool; the message protocol lives in
 * `src/worker/pyodide-worker.ts` (bundled to `dist/pyodide-worker.js`).
 */
export { PyodidePool, PyodideTaskError } from './pool/pyodide-pool.js'
export type {
  MapOptions,
  PyodideMapRun,
  PyodidePoolOptions,
  RunPythonOptions,
  WarmupResult,
} from './pool/pyodide-pool.js'
export type {
  ExecRequest,
  PingRequest,
  PingStatus,
  WorkerErrorInfo,
  WorkerFailure,
  WorkerRequest,
  WorkerResponse,
  WorkerSuccess,
} from './worker/pyodide-worker.js'
export type { WorkerPoolProgressCallback } from '@fideus-labs/worker-pool'
