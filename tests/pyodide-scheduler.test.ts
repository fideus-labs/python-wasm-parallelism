/**
 * Integration tests for the async dask scheduler (python/pyodide_pool/scheduler.py).
 *
 * Boots a REAL driver Pyodide, installs REAL dask (micropip, from PyPI — dask
 * is not in the Pyodide distribution), registers a real 2-worker PyodidePool
 * as `js_pyodide_pool`, and runs dask graphs through
 * `await pyodide_pool.get/compute(...)`, comparing against dask's own
 * synchronous scheduler. Workers receive dask automatically via package
 * mirroring (the driver installed it, so it rides in every task's snapshot);
 * beforeAll warms both workers concurrently so that install cost is paid
 * once, up front. Tests share one driver and one pool and run sequentially;
 * each driver snippet ends in a json.dumps(...) expression.
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
  await driver.loadPackage(['cloudpickle', 'micropip'], { messageCallback: () => {} })
  driver.FS.mkdirTree('/driver-site/pyodide_pool')
  for (const name of readdirSync(packageDir)) {
    if (!name.endsWith('.py')) continue
    driver.FS.writeFile(
      `/driver-site/pyodide_pool/${name}`,
      readFileSync(path.join(packageDir, name), 'utf8'),
    )
  }
  await driver.runPythonAsync(`
import sys
sys.path.insert(0, '/driver-site')
import micropip
await micropip.install("dask")
import asyncio, dask, pyodide_pool
# Two concurrent submits occupy both workers, so each mirrors dask (and the
# rest of the driver snapshot) now instead of skewing the timing tests.
await asyncio.gather(pyodide_pool.submit(lambda: 1), pyodide_pool.submit(lambda: 2))
`)
}, 240_000)

afterAll(() => {
  pool?.terminate()
})

it('get executes a raw legacy graph; literals and aliases stay local', async () => {
  const info = await run<{
    ours: string
    expected: string
    equal: boolean
    submits: number
  }>(`
import json, dask, pyodide_pool
import pyodide_pool._bridge as bridge

inc = lambda x: x + 1
add = lambda a, b: a + b
dsk = {
    "a": 1,
    "b": (inc, "a"),
    "c": (add, "b", "a"),
    "alias": "c",
    "lst": ["a", "alias"],
}
keys = [["b"], "c", "alias", "lst"]

calls = []
original = bridge.submit
async def counting(func, /, *args, **kwargs):
    calls.append(func)
    return await original(func, *args, **kwargs)
bridge.submit = counting
try:
    # num_workers must be accepted-and-ignored: the pool bounds concurrency.
    ours = await pyodide_pool.get(dsk, keys, num_workers=99)
finally:
    bridge.submit = original

expected = dask.get(dsk, keys)
json.dumps({
    "ours": repr(ours),
    "expected": repr(expected),
    "equal": ours == expected,
    "submits": len(calls),
})
`)
  expect(info.equal).toBe(true)
  expect(info.ours).toBe(info.expected)
  // Only "b" and "c" are tasks; "a" (literal), "alias", and "lst" (container)
  // must resolve on the driver without a worker round-trip.
  expect(info.submits).toBe(2)
})

it('compute(delayed) matches the synchronous scheduler', async () => {
  const info = await run<{
    expected: number
    single: number
    pair: number[]
    pair_is_tuple: boolean
    via_pool: number
  }>(`
import json, dask, pyodide_pool

def inc(x):
    return x + 1

def add(a, b):
    return a + b

leaves = [dask.delayed(inc)(i) for i in range(6)]
total = dask.delayed(sum)(leaves)

expected = total.compute(scheduler="synchronous")
single = await pyodide_pool.compute(total)
pair = await pyodide_pool.compute(total, dask.delayed(add)(1, 2), 7)
via_pool = await pyodide_pool.compute(total, pool=pyodide_pool.WorkerPool())
json.dumps({
    "expected": expected,
    "single": single,
    "pair": list(pair),
    "pair_is_tuple": isinstance(pair, tuple),
    "via_pool": via_pool,
})
`)
  expect(info.expected).toBe(21)
  expect(info.single).toBe(21)
  expect(info.pair).toEqual([21, 3, 7]) // non-dask 7 passes through repack
  expect(info.pair_is_tuple).toBe(true)
  expect(info.via_pool).toBe(21)
})

it('dispatches all ready tasks concurrently (worker intervals overlap)', async () => {
  const info = await run<{ n: number; overlaps: number }>(`
import json, dask, pyodide_pool

def timed_sleep(i):
    import time
    start = time.time()
    time.sleep(0.4)
    return (start, time.time())

intervals = await pyodide_pool.compute([dask.delayed(timed_sleep)(i) for i in range(4)])
overlaps = 0
for i in range(len(intervals)):
    for j in range(i + 1, len(intervals)):
        (s1, e1), (s2, e2) = intervals[i], intervals[j]
        if s1 < e2 and s2 < e1:
            overlaps += 1
json.dumps({"n": len(intervals), "overlaps": overlaps})
`)
  expect(info.n).toBe(4)
  // 4 x 0.4s tasks on 2 warm workers: sequential execution would produce 0
  // overlapping pairs; concurrent dispatch guarantees at least one.
  expect(info.overlaps).toBeGreaterThanOrEqual(1)
})

it('propagates the original exception, fails fast, and the pool survives', async () => {
  const info = await run<{
    raised: boolean
    message: string
    cause_type: string
    after: number
  }>(`
import json, dask, pyodide_pool

def boom():
    raise ValueError("scheduler boom")

def slow():
    import time
    time.sleep(0.3)
    return 1

total = dask.delayed(sum)([
    dask.delayed(slow)(),
    dask.delayed(boom)(),
    dask.delayed(slow)(),
])
try:
    await pyodide_pool.compute(total)
    outcome = {"raised": False, "message": "", "cause_type": ""}
except ValueError as exc:
    outcome = {
        "raised": True,
        "message": str(exc),
        "cause_type": type(exc.__cause__).__name__,
    }
outcome["after"] = await pyodide_pool.submit(lambda: 40 + 2)
json.dumps(outcome)
`)
  expect(info.raised).toBe(true)
  expect(info.message).toBe('scheduler boom')
  expect(info.cause_type).toBe('RemoteTraceback')
  expect(info.after).toBe(42)
})

it('computes a dask.bag reduction (modern Alias nodes resolve locally)', async () => {
  const info = await run<{ expected: number; ours: number }>(`
import json, dask, pyodide_pool
import dask.bag as db

def inc(x):
    return x + 1

bag = db.from_sequence(range(6), npartitions=3).map(inc).sum()
expected = bag.compute(scheduler="synchronous")
ours = await pyodide_pool.compute(bag)
json.dumps({"expected": expected, "ours": ours})
`)
  expect(info.expected).toBe(21)
  expect(info.ours).toBe(21)
})
