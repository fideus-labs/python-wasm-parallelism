/**
 * Integration tests for the multiprocessing shim (python/wasm_multiprocessing).
 *
 * Boots a REAL driver Pyodide wired to a real 2-worker PyodidePool via the
 * shared bootMultiprocessingDriver fixture (tests/helpers.ts) and exercises
 * the `multiprocessing.Pool`-shaped API end-to-end over the actual worker
 * bundle: batched amap chunks, AsyncResult lifecycle, streaming imap,
 * lifecycle/termination, and the capability-detected sync methods. Tests
 * share one driver and one pool and run sequentially; each driver snippet
 * ends in a json.dumps(...) expression so assertions stay independent of
 * PyProxy conversion rules. The context-manager test boots its own 1-worker
 * pool (terminate must be observable without disturbing the shared workers);
 * the porting test's `with Pool(...)` terminates the SHARED JS pool's idle
 * workers on exit, so it must stay last in the file.
 */
import { afterAll, beforeAll, expect, it } from 'vitest'
import type { PyodidePool } from '../src/index.js'
import { bootMultiprocessingDriver, createPool } from './helpers.js'
import type { PyodideDriver } from './helpers.js'

let pool: PyodidePool
let driver: PyodideDriver

async function run<T>(code: string): Promise<T> {
  return driver.run<T>(code)
}

beforeAll(async () => {
  pool = await createPool(2)
  driver = await bootMultiprocessingDriver(pool)
})

afterAll(() => {
  pool?.terminate()
})

it('amap matches builtins.map over 20 items with chunksize=5', async () => {
  // Pool(4) over the 2-worker JS pool takes the fast path (one mapPickled
  // batch); Pool(1) binds tighter than the JS pool and takes the
  // semaphore-gated per-chunk path — both must preserve input order.
  const outcome = await run<{ batched: number[]; gated: number[]; expected: number[] }>(`
import json
from wasm_multiprocessing import Pool

def square(x):
    return x * x

pool = Pool(4)
batched = await pool.amap(square, range(20), chunksize=5)
pool.close()
gated = Pool(1)
gated_result = await gated.amap(square, range(20), chunksize=5)
gated.close()
json.dumps({
    "batched": batched,
    "gated": gated_result,
    "expected": list(map(square, range(20))),
})
`)
  expect(outcome.expected).toEqual(Array.from({ length: 20 }, (_, x) => x * x))
  expect(outcome.batched).toEqual(outcome.expected)
  expect(outcome.gated).toEqual(outcome.expected)
})

it('apply_async: awaited get returns the value, ready flips, failures re-raise', async () => {
  const outcome = await run<{
    ready_before: boolean
    value: number
    ready_after: boolean
    successful: boolean
    fail_successful: boolean
    reraised_type: string
    reraised_message: string
    cause_type: string
  }>(`
import json
from wasm_multiprocessing import Pool

def square(x):
    return x * x

def boom(x):
    raise ValueError(f"boom-{x}")

pool = Pool(2)

r = pool.apply_async(square, (7,))
ready_before = r.ready()
value = await r  # the awaited form of get()
outcome = {
    "ready_before": ready_before,
    "value": value,
    "ready_after": r.ready(),
    "successful": r.successful(),
}

f = pool.apply_async(boom, (3,))
await f.await_ready()
outcome["fail_successful"] = f.successful()
try:
    await f.aget()
    outcome["reraised_type"] = "<no exception>"
except ValueError as exc:  # the ORIGINAL type, not a wrapper
    outcome["reraised_type"] = type(exc).__name__
    outcome["reraised_message"] = str(exc)
    outcome["cause_type"] = type(exc.__cause__).__name__
pool.close()
json.dumps(outcome)
`)
  expect(outcome.ready_before).toBe(false)
  expect(outcome.value).toBe(49)
  expect(outcome.ready_after).toBe(true)
  expect(outcome.successful).toBe(true)
  expect(outcome.fail_successful).toBe(false)
  expect(outcome.reraised_type).toBe('ValueError')
  expect(outcome.reraised_message).toBe('boom-3')
  expect(outcome.cause_type).toBe('RemoteTraceback')
})

it('imap preserves input order; imap_unordered yields everything', async () => {
  const outcome = await run<{ ordered: number[]; unordered: number[] }>(`
import json
from wasm_multiprocessing import Pool

def square(x):
    return x * x

pool = Pool(2)
ordered = [v async for v in pool.imap(square, range(10), chunksize=2)]
unordered = [v async for v in pool.imap_unordered(square, range(10), chunksize=3)]
pool.close()
json.dumps({"ordered": ordered, "unordered": unordered})
`)
  const expected = Array.from({ length: 10 }, (_, x) => x * x)
  expect(outcome.ordered).toEqual(expected)
  // Completion order is allowed to differ — only the multiset must match.
  expect([...outcome.unordered].sort((a, b) => a - b)).toEqual(expected)
  expect(outcome.unordered).toHaveLength(expected.length)
})

it('context-manager exit terminates the workers (ping/status proof)', async () => {
  // Dedicated 1-worker pool so the kill is observable per the recycling
  // contract (tests/pool.test.ts): a ping-backed warmup on a still-alive
  // interpreter reports bootMs === 0; only a terminated worker pays a boot.
  const ctxPool = await createPool(1)
  try {
    const [fresh] = await ctxPool.warmup()
    expect(fresh?.bootMs).toBeGreaterThan(0)

    driver.api.registerJsModule('js_mp_ctx_pool', { pool: ctxPool })
    const outcome = await run<{ mapped: number[]; rejected: boolean }>(`
import json, pyodide_pool
import js_mp_ctx_pool
from wasm_multiprocessing import Pool

def square(x):
    return x * x

with Pool(2, pool=pyodide_pool.WorkerPool(js_mp_ctx_pool.pool)) as p:
    mapped = await p.amap(square, [1, 2, 3])
try:
    p.apply_async(square, (1,))
    rejected = False
except ValueError:  # stdlib __exit__ semantics: the pool is TERMINATE'd
    rejected = True
json.dumps({"mapped": mapped, "rejected": rejected})
`)
    expect(outcome.mapped).toEqual([1, 4, 9])
    expect(outcome.rejected).toBe(true)

    // The amap ran on the warm interpreter; had __exit__ not terminated it,
    // this warmup would find it recycled (bootMs === 0). A fresh boot proves
    // the JS-side workers were actually killed, not just the state flag set.
    const [after] = await ctxPool.warmup()
    expect(after?.bootMs).toBeGreaterThan(0)
  } finally {
    ctxPool.terminate()
  }
})

it('sync map on flagless Node raises the guidance error (design-doc contract)', async () => {
  // docs/architecture/multiprocessing-shim-design.md commits sync methods to
  // capability detection: run_sync under JSPI, otherwise a RuntimeError
  // naming the exact async replacement. Default (flagless) Node cannot stack
  // switch, so this suite must see the error — if can_run_sync() ever flips
  // true here, the environment changed and this assertion should be revisited.
  const outcome = await run<{ can_run_sync: boolean; error_type: string; message: string }>(`
import json
from pyodide.ffi import can_run_sync
from wasm_multiprocessing import Pool

def square(x):
    return x * x

pool = Pool(2)
outcome = {"can_run_sync": can_run_sync()}
try:
    pool.map(square, range(4))
    outcome["error_type"] = "<no exception>"
except RuntimeError as exc:
    outcome["error_type"] = type(exc).__name__
    outcome["message"] = str(exc)
pool.close()
json.dumps(outcome)
`)
  expect(outcome.can_run_sync).toBe(false)
  expect(outcome.error_type).toBe('RuntimeError')
  expect(outcome.message).toContain('await pool.amap(func, iterable)')
  expect(outcome.message).toContain('JSPI')
})

it('ports a stdlib multiprocessing snippet with an import change + awaited entry', async () => {
  // The classic count-primes example as written against the stdlib —
  // `from multiprocessing import Pool`, sync main(), pool.map — ported per
  // the design doc's flagless-Node recipe: swap the import line, make the
  // entry point async and await it, and use the awaited map form (under
  // JSPI the original pool.map line would run verbatim instead).
  const total = await run<number>(`
import json
from wasm_multiprocessing import Pool  # was: from multiprocessing import Pool

def count_primes(bounds):
    lo, hi = bounds
    def is_prime(n):
        if n < 2:
            return False
        i = 2
        while i * i <= n:
            if n % i == 0:
                return False
            i += 1
        return True
    return sum(1 for n in range(lo, hi) if is_prime(n))

async def main():  # was: def main()
    with Pool(2) as pool:
        counts = await pool.amap(count_primes, [(0, 1000), (1000, 2000), (2000, 3000)])
    return sum(counts)

json.dumps(await main())  # was: json.dumps(main())
`)
  // pi(3000) = 430 primes below 3000.
  expect(total).toBe(430)
})
