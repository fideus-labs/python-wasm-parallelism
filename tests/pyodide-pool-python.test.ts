/**
 * Integration tests for the driver-side Python package (python/pyodide_pool).
 *
 * Boots a REAL driver Pyodide in the (fork's) main thread wired to a real
 * 2-worker PyodidePool via the shared bootDriver fixture (tests/helpers.ts)
 * and exercises the bridge end-to-end: cloudpickle payloads out, results and
 * remote exceptions back — the exact topology of
 * docs/architecture/dask-scheduler-design.md. Tests share one driver and one
 * pool and run sequentially; each driver snippet ends in a json.dumps(...)
 * expression so assertions stay independent of PyProxy conversion rules.
 * The package-mirroring test boots its own 1-worker pool (and loads numpy
 * into the driver), so it must stay last in the file.
 */
import { afterAll, beforeAll, expect, it } from 'vitest'
import type { PyodidePool } from '../src/index.js'
import { bootDriver, createPool } from './helpers.js'
import type { PyodideDriver } from './helpers.js'

let pool: PyodidePool
let driver: PyodideDriver

async function run<T>(code: string): Promise<T> {
  return driver.run<T>(code)
}

beforeAll(async () => {
  // bootDriver loads cloudpickle for the bridge and micropip so
  // snapshot_packages exercises a real micropip.list() (both are
  // excluded-from-mirror names), and "installs" python/pyodide_pool by
  // writing its sources into the driver's FS.
  pool = await createPool(2)
  driver = await bootDriver(pool)
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

it('mirrors numpy to workers automatically; snapshot replay is idempotent', async () => {
  // Dedicated 1-worker pool so both submits deterministically hit the SAME
  // worker: the first submit must trigger the mirrored numpy install, the
  // second must find the worker's installed-set already warm — replay being
  // cheap and idempotent is the design's substitute for a driver/worker
  // package-sync protocol. (The wheels/micropip mirror path gets the same
  // treatment from the scheduler suite, which mirrors PyPI dask to workers.)
  const numpyPool = await createPool(1)
  try {
    const [fresh] = await numpyPool.warmup()
    expect(fresh?.status.loadedPackages).not.toContain('numpy')

    await driver.api.loadPackage('numpy', { messageCallback: () => {} })
    driver.api.registerJsModule('js_numpy_pool', { pool: numpyPool })
    const info = await run<{
      snapshot_has_numpy: boolean
      snapshot_calls: number
      first: number[][]
      second: number[][]
      expected: number[][]
      first_type: string
      first_s: number
      second_s: number
    }>(`
import json, time
import numpy as np
import js_numpy_pool
import pyodide_pool
import pyodide_pool._bridge as _bridge

pool = pyodide_pool.WorkerPool(js_numpy_pool.pool)

# Every submit must ride a FRESH snapshot on its execPickled message (no
# separate sync machinery), so snapshot_packages is called once per submit.
calls = {"n": 0}
_orig_snapshot = _bridge.snapshot_packages
def _counting_snapshot():
    calls["n"] += 1
    return _orig_snapshot()
_bridge.snapshot_packages = _counting_snapshot

# Lambda closes over the driver's numpy module; cloudpickle ships the module
# by reference, so unpickling on the worker imports numpy there — which only
# works because the mirrored snapshot installed it before unpickling.
use_numpy = lambda: np.arange(6, dtype=np.int64).reshape(2, 3) * 2

try:
    t0 = time.perf_counter()
    first = await pool.submit(use_numpy)
    t1 = time.perf_counter()
    second = await pool.submit(use_numpy)
    t2 = time.perf_counter()
finally:
    _bridge.snapshot_packages = _orig_snapshot

json.dumps({
    "snapshot_has_numpy": "numpy" in pyodide_pool.snapshot_packages().packages,
    "snapshot_calls": calls["n"],
    "first": first.tolist(),
    "second": second.tolist(),
    "expected": use_numpy().tolist(),
    "first_type": type(first).__name__,
    "first_s": t1 - t0,
    "second_s": t2 - t1,
})
`)
    expect(info.snapshot_has_numpy).toBe(true)
    expect(info.snapshot_calls).toBe(2)
    expect(info.first_type).toBe('ndarray')
    expect(info.first).toEqual(info.expected)
    expect(info.second).toEqual(info.expected)
    // Idempotent replay: the first submit paid for the worker's numpy
    // download + install (seconds); the second found pyodide.loadedPackages
    // warm and skipped straight to execution.
    expect(info.second_s).toBeLessThan(info.first_s / 2)

    // The ndarray round-trip proves numpy works on the worker; the worker's
    // own interpreter status proves it was installed THERE, not just locally.
    const [after] = await numpyPool.warmup()
    expect(after?.status.loadedPackages).toContain('numpy')
  } finally {
    numpyPool.terminate()
  }
})
