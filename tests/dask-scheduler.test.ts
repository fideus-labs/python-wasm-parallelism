/**
 * Dask scheduler + package-mirroring test suite
 * (python/pyodide_pool/scheduler.py and python/pyodide_pool/_packages.py).
 *
 * Boots a REAL driver Pyodide in the Node main thread wired to a real
 * 2-worker PyodidePool — the exact topology of examples/node-dask-demo.ts,
 * bootstrapped via the shared bootDaskDriver fixture (tests/helpers.ts):
 * dask installs from PyPI via micropip (it is not in the Pyodide
 * distribution) and both workers mirror the driver's snapshot up front.
 * Graph-shape tests assert `await pyodide_pool.compute(...)` equals dask's
 * own synchronous scheduler on the same graph. Tests share one driver and
 * one pool and run sequentially; each driver snippet ends in a
 * json.dumps(...) expression. The numpy package-mirroring test loads numpy
 * into the driver — every later submit would mirror it — so it stays LAST.
 */
import { afterAll, beforeAll, expect, it } from 'vitest'
import type { PyodidePool } from '../src/index.js'
import { bootDaskDriver, createPool } from './helpers.js'
import type { PyodideDriver } from './helpers.js'

const POOL_SIZE = 2

let pool: PyodidePool
let driver: PyodideDriver

async function run<T>(code: string): Promise<T> {
  return driver.run<T>(code)
}

beforeAll(async () => {
  pool = await createPool(POOL_SIZE)
  driver = await bootDaskDriver(pool)
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

it('literal graph values and zero-task graphs resolve without workers', async () => {
  const info = await run<{
    empty: unknown[]
    literals: [number[], number]
    plain: number
    containers: number[]
    submits: number
    literal_delayed: number
  }>(`
import json, dask, pyodide_pool
import pyodide_pool._bridge as bridge

calls = []
original = bridge.submit
async def counting(func, /, *args, **kwargs):
    calls.append(func)
    return await original(func, *args, **kwargs)
bridge.submit = counting
try:
    empty = await pyodide_pool.get({}, [])
    literals = await pyodide_pool.get({"x": 41, "y": "x", "z": ["x", "y"]}, ["z", "x"])
    plain = await pyodide_pool.compute(123)
    containers = await pyodide_pool.compute([1, 2, 3])
finally:
    bridge.submit = original
zero_task_submits = len(calls)

# dask.delayed(5) wraps a literal; whether dask's finalize layer adds a
# task node is version-dependent, so only the value is asserted here.
literal_delayed = await pyodide_pool.compute(dask.delayed(5))
json.dumps({
    "empty": empty,
    "literals": literals,
    "plain": plain,
    "containers": containers,
    "submits": zero_task_submits,
    "literal_delayed": literal_delayed,
})
`)
  expect(info.empty).toEqual([])
  expect(info.literals).toEqual([[41, 41], 41])
  expect(info.plain).toBe(123)
  expect(info.containers).toEqual([1, 2, 3])
  expect(info.submits).toBe(0)
  expect(info.literal_delayed).toBe(5)
})

it('independent leaves + reduction matches the synchronous scheduler', async () => {
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

expected = dask.compute(total, scheduler="synchronous")[0]
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

it('diamond dependency graph matches the synchronous scheduler', async () => {
  const info = await run<{ expected: number; ours: number }>(`
import json, dask, pyodide_pool

def inc(x):
    return x + 1

def double(x):
    return 2 * x

def add(a, b):
    return a + b

root = dask.delayed(inc)(10)
left = dask.delayed(double)(root)
right = dask.delayed(inc)(root)
top = dask.delayed(add)(left, right)

expected = dask.compute(top, scheduler="synchronous")[0]
ours = await pyodide_pool.compute(top)
json.dumps({"expected": expected, "ours": ours})
`)
  expect(info.expected).toBe(34) // add(double(11), inc(11))
  expect(info.ours).toBe(34)
})

it('nested delayed calls match the synchronous scheduler', async () => {
  const info = await run<{ expected: number; ours: number }>(`
import json, dask, pyodide_pool

def inc(x):
    return x + 1

def double(x):
    return 2 * x

# Delayed objects nested directly as arguments of other delayed calls, plus
# a container mixing delayed results with a literal.
inner = dask.delayed(inc)(dask.delayed(inc)(dask.delayed(inc)(0)))
mixed = dask.delayed(sum)([inner, dask.delayed(double)(inner), 4])

expected = dask.compute(mixed, scheduler="synchronous")[0]
ours = await pyodide_pool.compute(mixed)
json.dumps({"expected": expected, "ours": ours})
`)
  expect(info.expected).toBe(13) // 3 + 6 + 4
  expect(info.ours).toBe(13)
})

it('dask.bag map/filter/sum matches the synchronous scheduler', async () => {
  const info = await run<{ expected: number; ours: number }>(`
import json, dask, pyodide_pool
import dask.bag as db

bag = (
    db.from_sequence(range(12), npartitions=3)
    .map(lambda x: x * x)
    .filter(lambda x: x % 2 == 0)
    .sum()
)
expected = dask.compute(bag, scheduler="synchronous")[0]
ours = await pyodide_pool.compute(bag)
json.dumps({"expected": expected, "ours": ours})
`)
  expect(info.expected).toBe(220) // even squares of 0..11
  expect(info.ours).toBe(220)
})

it('propagates the original exception and traceback, fails fast, pool survives', async () => {
  const info = await run<{
    raised: boolean
    message: string
    cause_type: string
    cause_text: string
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
    outcome = {"raised": False, "message": "", "cause_type": "", "cause_text": ""}
except ValueError as exc:
    outcome = {
        "raised": True,
        "message": str(exc),
        "cause_type": type(exc.__cause__).__name__,
        "cause_text": str(exc.__cause__),
    }
outcome["after"] = await pyodide_pool.submit(lambda: 40 + 2)
json.dumps(outcome)
`)
  // The ORIGINAL exception type re-raises on the driver (the except clause
  // catching ValueError proves it), with the worker traceback as its cause.
  expect(info.raised).toBe(true)
  expect(info.message).toBe('scheduler boom')
  expect(info.cause_type).toBe('RemoteTraceback')
  expect(info.cause_text).toContain('ValueError: scheduler boom')
  expect(info.cause_text).toContain('boom') // the raising frame's name
  expect(info.after).toBe(42)
})

it('bounds in-flight tasks at poolSize (6 ready tasks, 2 workers)', async () => {
  const info = await run<{ n: number; workers: number; peak: number }>(`
import json, dask, pyodide_pool

def probe(i):
    # Runs on a worker. Tag the interpreter with a persistent id (recycled
    # workers keep their interpreter, so the tag survives across tasks) and
    # report this task's wall-clock execution interval.
    import builtins, time, uuid
    if not hasattr(builtins, "_pool_probe_wid"):
        builtins._pool_probe_wid = uuid.uuid4().hex
    start = time.time()
    time.sleep(0.4)
    return (builtins._pool_probe_wid, start, time.time())

results = await pyodide_pool.compute([dask.delayed(probe)(i) for i in range(6)])

# Sweep the execution intervals for the peak number running at once. Ties
# sort ends (-1) before starts (+1), which can only under-count the peak —
# safe for the upper bound being asserted.
events = []
for _, start, end in results:
    events.append((start, 1))
    events.append((end, -1))
peak = running = 0
for _, delta in sorted(events):
    running += delta
    peak = max(peak, running)

json.dumps({
    "n": len(results),
    "workers": len({wid for wid, _, _ in results}),
    "peak": peak,
})
`)
  expect(info.n).toBe(6)
  // All 6 tasks must land on exactly the pool's 2 interpreters (each worker
  // executes strictly one request at a time)...
  expect(info.workers).toBe(2)
  // ...and the interval sweep must show 2 at the peak: >=2 proves ready
  // tasks dispatch concurrently, <=2 proves in-flight work never exceeds
  // poolSize.
  expect(info.peak).toBe(2)
})

// LAST: loads numpy into the driver, which every later submit would mirror.
it('mirrors numpy to workers for delayed tasks; install replay is cached', async () => {
  await driver.api.loadPackage('numpy', { messageCallback: () => {} })
  const info = await run<{
    first_ok: boolean
    second_ok: boolean
    first_s: number
    second_s: number
  }>(`
import json, time, dask
import numpy as np
import pyodide_pool

def norm_of_arange(n):
    # np is a module reference, pickled BY REFERENCE — the worker can only
    # unpickle this payload after package mirroring installs numpy there.
    return float(np.linalg.norm(np.arange(n, dtype=np.float64)))

n = 250_000
expected = norm_of_arange(n)
batch = [dask.delayed(norm_of_arange)(n) for _ in range(${POOL_SIZE})]

t0 = time.perf_counter()
first = await pyodide_pool.compute(batch)
first_s = time.perf_counter() - t0

t0 = time.perf_counter()
second = await pyodide_pool.compute(batch)
second_s = time.perf_counter() - t0

json.dumps({
    "first_ok": all(v == expected for v in first),
    "second_ok": all(v == expected for v in second),
    "first_s": first_s,
    "second_s": second_s,
})
`)
  expect(info.first_ok).toBe(true)
  expect(info.second_ok).toBe(true)
  // Idempotent replay: the first batch paid each worker's numpy install
  // (seconds); the second found the worker's installed-set warm and went
  // straight to execution. The /2 margin absorbs scheduling jitter without
  // weakening the install-vs-replay distinction (orders of magnitude apart).
  expect(info.second_s).toBeLessThan(info.first_s / 2)

  // Structural proof the install happened on the WORKERS, not just locally:
  // both interpreters now report numpy among their loaded packages.
  const statuses = await pool.warmup()
  expect(statuses).toHaveLength(POOL_SIZE)
  for (const { status } of statuses) {
    expect(status.loadedPackages).toContain('numpy')
  }
})
