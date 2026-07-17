/**
 * Shared fixtures for the Node test suites.
 *
 * Every suite drives the real worker bundle (dist/pyodide-worker.js) rather
 * than mocking the protocol, so the bundle build from scripts/build.mjs is
 * reproduced here exactly once per vitest fork — suites call
 * buildWorkerBundle() in beforeAll and get the same artifact the demos and
 * the library build ship. vitest runs test files sequentially
 * (fileParallelism: false in vitest.config.ts), so rebuilding the same
 * outfile from different files cannot race.
 */
import { readFileSync, readdirSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { build } from 'esbuild'
import { loadPyodide } from 'pyodide'
import type { PyodideAPI } from 'pyodide'
import { PyodidePool } from '../src/index.js'
import type { PyodidePoolOptions } from '../src/index.js'

export const rootDir = fileURLToPath(new URL('..', import.meta.url))

/** Source directory of the driver-side Python package (python/pyodide_pool). */
export const packageDir = path.join(rootDir, 'python', 'pyodide_pool')

/** Absolute path of the worker bundle the suites (and demos) load. */
export const workerFile = path.join(rootDir, 'dist', 'pyodide-worker.js')

/**
 * file:// URL of the worker bundle — the `web-worker` polyfill resolves
 * relative paths against process.cwd(), so pools must get an absolute URL.
 */
export const workerFileUrl = pathToFileURL(workerFile)

let bundlePromise: Promise<URL> | null = null

/**
 * Bundle src/worker/pyodide-worker.ts with the same options as
 * scripts/build.mjs (minus sourcemap and logging) and resolve with the
 * bundle's file:// URL. Memoized: at most one build per process no matter
 * how many fixtures ask.
 */
export function buildWorkerBundle(): Promise<URL> {
  bundlePromise ??= build({
    bundle: true,
    format: 'esm',
    platform: 'neutral',
    target: 'es2022',
    entryPoints: [path.join(rootDir, 'src', 'worker', 'pyodide-worker.ts')],
    outfile: workerFile,
    external: ['pyodide'],
    logLevel: 'silent',
  }).then(() => workerFileUrl)
  return bundlePromise
}

/** Create a PyodidePool wired to the freshly built worker bundle. */
export async function createPool(
  poolSize: number,
  options: Omit<PyodidePoolOptions, 'poolSize' | 'workerUrl'> = {},
): Promise<PyodidePool> {
  return new PyodidePool({ poolSize, workerUrl: await buildWorkerBundle(), ...options })
}

/**
 * Run `fn` with a fresh pool of the given size; terminate() is guaranteed
 * afterward so a failing test never leaks worker_threads that keep the
 * vitest fork alive.
 */
export async function withPool<T>(
  poolSize: number,
  fn: (pool: PyodidePool) => Promise<T>,
  options: Omit<PyodidePoolOptions, 'poolSize' | 'workerUrl'> = {},
): Promise<T> {
  const pool = await createPool(poolSize, options)
  try {
    return await fn(pool)
  } finally {
    pool.terminate()
  }
}

/**
 * `cloudpickle.dumps(<expr>)` evaluated on the pool itself and returned as
 * payload bytes (shipped back as a list of ints — always structured-clone-
 * safe), so pickled-protocol tests stay independent of any host-side pickle
 * implementation.
 */
export async function pickleCall(pool: PyodidePool, expr: string): Promise<ArrayBuffer> {
  const bytes = await pool.runPython<number[]>(
    `import cloudpickle\nlist(cloudpickle.dumps(${expr}))`,
    { packages: ['cloudpickle'] },
  )
  return new Uint8Array(bytes).buffer
}

/**
 * cloudpickle.loads a payload on the pool and evaluate `expr` over the
 * unpickled `obj` (default: the object itself).
 */
export async function unpickle<T>(
  pool: PyodidePool,
  payload: ArrayBuffer,
  expr = 'obj',
): Promise<T> {
  return pool.runPython<T>(`import cloudpickle\nobj = cloudpickle.loads(bytes(data))\n${expr}`, {
    globals: { data: Array.from(new Uint8Array(payload)) },
    packages: ['cloudpickle'],
  })
}

/** A driver Pyodide booted in the (fork's) main thread, wired to a pool. */
export interface PyodideDriver {
  /** The raw driver instance (loadPackage, registerJsModule, FS, ...). */
  api: PyodideAPI
  /** Run driver Python ending in a json.dumps(...) expression; parse it. */
  run<T>(code: string): Promise<T>
}

/**
 * Boot a REAL driver Pyodide in the main thread and wire it to `pool` — the
 * exact topology of examples/node-dask-demo.ts: register the pool as
 * `js_pyodide_pool`, load cloudpickle (bridge) + micropip (mirroring),
 * "install" python/pyodide_pool by writing its sources into the driver's FS,
 * and import it. Assertions should go through `run` so they stay independent
 * of PyProxy conversion rules.
 */
export async function bootDriver(pool: PyodidePool): Promise<PyodideDriver> {
  const api = await loadPyodide()
  api.registerJsModule('js_pyodide_pool', { pool })
  await api.loadPackage(['cloudpickle', 'micropip'], { messageCallback: () => {} })
  api.FS.mkdirTree('/driver-site/pyodide_pool')
  for (const name of readdirSync(packageDir)) {
    if (!name.endsWith('.py')) continue
    api.FS.writeFile(
      `/driver-site/pyodide_pool/${name}`,
      readFileSync(path.join(packageDir, name), 'utf8'),
    )
  }
  await api.runPythonAsync("import sys; sys.path.insert(0, '/driver-site'); import pyodide_pool")
  return {
    api,
    run: async <T>(code: string): Promise<T> => {
      const result: unknown = await api.runPythonAsync(code)
      return JSON.parse(String(result)) as T
    },
  }
}

/**
 * {@link bootDriver}, then install dask from PyPI via micropip (dask is NOT
 * in the Pyodide distribution) and occupy every worker once so each mirrors
 * the driver's snapshot (dask and its dependencies) up front instead of
 * skewing the first timing-sensitive test.
 */
export async function bootDaskDriver(pool: PyodidePool): Promise<PyodideDriver> {
  const handle = await bootDriver(pool)
  await handle.api.runPythonAsync(`
import asyncio, micropip
await micropip.install("dask")
import dask, pyodide_pool
await asyncio.gather(*(pyodide_pool.submit(lambda i=i: i) for i in range(${pool.poolSize})))
`)
  return handle
}
