/**
 * Integration tests for the driver-side Python package (python/pyodide_pool).
 *
 * Boots a REAL driver Pyodide in the (fork's) main thread, registers a real
 * 2-worker PyodidePool as `js_pyodide_pool`, writes the package sources into
 * the driver's FS, and exercises the bridge end-to-end: cloudpickle payloads
 * out, results and remote exceptions back — the exact topology of
 * docs/architecture/dask-scheduler-design.md. Tests share one driver and one
 * pool and run sequentially; each driver snippet ends in a json.dumps(...)
 * expression so assertions stay independent of PyProxy conversion rules.
 */
import { readFileSync, readdirSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { build } from 'esbuild'
import { loadPyodide } from 'pyodide'
import type { PyodideAPI } from 'pyodide'
import { afterAll, beforeAll, expect, it } from 'vitest'
import { PyodidePool } from '../src/index.js'

const rootDir = fileURLToPath(new URL('..', import.meta.url))
const workerFile = path.join(rootDir, 'dist', 'pyodide-worker.js')
const packageDir = path.join(rootDir, 'python', 'pyodide_pool')

let pool: PyodidePool
let driver: PyodideAPI

/** Run driver Python ending in a json.dumps(...) expression; parse it. */
async function run<T>(code: string): Promise<T> {
  const result: unknown = await driver.runPythonAsync(code)
  return JSON.parse(String(result)) as T
}

beforeAll(async () => {
  await build({
    bundle: true,
    format: 'esm',
    platform: 'neutral',
    target: 'es2022',
    entryPoints: [path.join(rootDir, 'src', 'worker', 'pyodide-worker.ts')],
    outfile: workerFile,
    external: ['pyodide'],
    logLevel: 'silent',
  })
  pool = new PyodidePool({ poolSize: 2, workerUrl: pathToFileURL(workerFile) })
  driver = await loadPyodide()
  driver.registerJsModule('js_pyodide_pool', { pool })
  // cloudpickle for the bridge; micropip so snapshot_packages exercises a
  // real micropip.list() (both are excluded-from-mirror names).
  await driver.loadPackage(['cloudpickle', 'micropip'], { messageCallback: () => {} })
  // "Install" the package by writing its sources into the driver's FS.
  driver.FS.mkdirTree('/driver-site/pyodide_pool')
  for (const name of readdirSync(packageDir)) {
    if (!name.endsWith('.py')) continue
    driver.FS.writeFile(
      `/driver-site/pyodide_pool/${name}`,
      readFileSync(path.join(packageDir, name), 'utf8'),
    )
  }
  await driver.runPythonAsync(
    "import sys; sys.path.insert(0, '/driver-site'); import pyodide_pool",
  )
})

afterAll(() => {
  pool?.terminate()
})

it('exposes the public API and registers itself pickle-by-value', async () => {
  const info = await run<{ all: string[]; registry: string[] }>(`
import json, cloudpickle, pyodide_pool
json.dumps({
    "all": sorted(pyodide_pool.__all__),
    "registry": sorted(cloudpickle.list_registry_pickle_by_value()),
})
`)
  expect(info.all).toEqual(
    expect.arrayContaining(['WorkerPool', 'compute', 'get', 'submit', 'snapshot_packages']),
  )
  expect(info.registry).toContain('pyodide_pool')
})

it('submit runs a call with args and kwargs on a worker', async () => {
  const result = await run<number[]>(`
import json, pyodide_pool
result = await pyodide_pool.submit(sorted, [3, 1, 2], reverse=True)
json.dumps(result)
`)
  expect(result).toEqual([3, 2, 1])
})

it('WorkerPool wraps the registered JS pool and ships lambdas by value', async () => {
  const info = await run<{ pool_size: number; value: number }>(`
import json, pyodide_pool
pool = pyodide_pool.WorkerPool()
value = await pool.submit(lambda a, b: a * b, 6, 7)
json.dumps({"pool_size": pool.pool_size, "value": value})
`)
  expect(info.pool_size).toBe(2)
  expect(info.value).toBe(42)
})

it('re-raises the original remote exception with a RemoteTraceback cause', async () => {
  const outcome = await run<{
    raised: boolean
    message: string
    cause_type: string
    cause_text: string
  }>(`
import json, pyodide_pool

def fail_remotely():
    raise ValueError("boom from worker")

try:
    await pyodide_pool.submit(fail_remotely)
    outcome = {"raised": False}
except ValueError as exc:
    outcome = {
        "raised": True,
        "message": str(exc),
        "cause_type": type(exc.__cause__).__name__,
        "cause_text": str(exc.__cause__),
    }
json.dumps(outcome)
`)
  expect(outcome.raised).toBe(true)
  expect(outcome.message).toBe('boom from worker')
  expect(outcome.cause_type).toBe('RemoteTraceback')
  expect(outcome.cause_text).toContain('ValueError: boom from worker')
  expect(outcome.cause_text).toContain('fail_remotely')
})

it('falls back to RemoteExecutionError when the exception cannot be pickled', async () => {
  const outcome = await run<{ raised: string; message: string; traceback: string }>(`
import json, pyodide_pool

class Unpicklable(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.baggage = (i for i in range(3))  # generators cannot be pickled

def fail_unpicklably():
    raise Unpicklable("cannot travel")

try:
    await pyodide_pool.submit(fail_unpicklably)
    outcome = {"raised": "", "message": "", "traceback": ""}
except pyodide_pool.RemoteExecutionError as exc:
    outcome = {
        "raised": type(exc).__name__,
        "message": str(exc),
        "traceback": exc.remote_traceback or "",
    }
json.dumps(outcome)
`)
  expect(outcome.raised).toBe('RemoteExecutionError')
  expect(outcome.message).toContain('cannot travel')
  expect(outcome.traceback).toContain('Unpicklable')
  expect(outcome.traceback).toContain('fail_unpicklably')
})

it('pool keeps serving after remote failures (workers recycled, not lost)', async () => {
  const result = await run<number>(`
import json, pyodide_pool
json.dumps(await pyodide_pool.submit(lambda: 40 + 2))
`)
  expect(result).toBe(42)
})

it('snapshot_packages splits distribution/wheels and filters excluded names', async () => {
  // Fake one distribution package and one wheel install so the split is
  // observable without network; removed again so later submits stay clean.
  const snapshot = await run<{ packages: string[]; wheels: string[] }>(`
import json, pyodide_js, pyodide_pool
pyodide_js.loadedPackages.fake_dist_pkg = "default channel"
pyodide_js.loadedPackages.fake_wheel_pkg = "https://example.com/fake_wheel_pkg-1.0-py3-none-any.whl"
try:
    packages, wheels = pyodide_pool.snapshot_packages()
finally:
    delattr(pyodide_js.loadedPackages, "fake_dist_pkg")
    delattr(pyodide_js.loadedPackages, "fake_wheel_pkg")
json.dumps({"packages": packages, "wheels": wheels})
`)
  // cloudpickle and micropip ARE genuinely installed (and appear in both
  // pyodide.loadedPackages and micropip.list()), so exact equality proves
  // the excluded names were filtered from both snapshot sources.
  expect(snapshot.packages).toEqual(['fake_dist_pkg'])
  expect(snapshot.wheels).toEqual(['https://example.com/fake_wheel_pkg-1.0-py3-none-any.whl'])
})
