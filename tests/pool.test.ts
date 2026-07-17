/**
 * Integration tests for PyodidePool over the real worker bundle.
 *
 * tests/helpers.ts builds dist/pyodide-worker.js exactly like
 * scripts/build.mjs; this suite then drives a real 2-worker pool in Node —
 * the environment-aware factory resolves the `web-worker` polyfill because
 * Node has no global Worker. One pool is shared across tests to amortize
 * interpreter boots; the warmup test runs first because it asserts on fresh
 * (bootMs > 0) workers. Tests that need their own pool size use withPool(),
 * which guarantees terminate().
 */
import path from 'node:path'
import { pathToFileURL } from 'node:url'
import { afterAll, beforeAll, expect, it } from 'vitest'
import { PyodidePool, PyodideTaskError } from '../src/index.js'
import { createPool, pickleCall, rootDir, unpickle, withPool } from './helpers.js'

let pool: PyodidePool

beforeAll(async () => {
  pool = await createPool(2)
})

afterAll(() => {
  pool?.terminate()
})

it('rejects invalid pool sizes', () => {
  expect(() => new PyodidePool({ poolSize: 0 })).toThrow(RangeError)
  expect(() => new PyodidePool({ poolSize: 1.5 })).toThrow(RangeError)
})

it('warmup boots exactly poolSize interpreters in parallel (ping status)', async () => {
  const results = await pool.warmup()
  expect(results).toHaveLength(2)
  for (const result of results) {
    expect(result.status.booted).toBe(true)
    expect(result.bootMs).toBeGreaterThan(0)
    expect(result.status.pyodideVersion).toBeTypeOf('string')
  }
})

it('runPython returns the final expression value', async () => {
  await expect(pool.runPython<number>('sum(range(10))')).resolves.toBe(45)
})

it('runPython returns scalar, list, and dict results', async () => {
  await expect(pool.runPython<number>('7 / 2')).resolves.toBe(3.5)
  await expect(pool.runPython<string>("'-'.join(['a', 'b'])")).resolves.toBe('a-b')
  await expect(pool.runPython<boolean>('10 > 3')).resolves.toBe(true)
  await expect(pool.runPython<number[]>('[x * x for x in range(4)]')).resolves.toEqual([0, 1, 4, 9])
  await expect(
    pool.runPython("{'name': 'pool', 'sizes': [1, 2], 'flags': {'warm': True}}"),
  ).resolves.toEqual({ name: 'pool', sizes: [1, 2], flags: { warm: true } })
})

it('runPython injects globals', async () => {
  await expect(pool.runPython<number>('a * b', { globals: { a: 6, b: 7 } })).resolves.toBe(42)
})

it('runPython rejects with PyodideTaskError carrying the Python traceback', async () => {
  const error = await pool.runPython('1 / 0').catch((err: unknown) => err)
  expect(error).toBeInstanceOf(PyodideTaskError)
  const taskError = error as PyodideTaskError
  expect(taskError.message).toContain('ZeroDivisionError')
  expect(taskError.pythonTraceback).toContain('Traceback')
})

it('pool keeps serving after a failed task (worker was recycled, not lost)', async () => {
  await expect(pool.runPython<number>('40 + 2')).resolves.toBe(42)
})

it('map over N > poolSize items returns ordered results (template injects item and index)', async () => {
  const run = pool.map<number, number>('item * 10 + index', [5, 6, 7, 8])
  await expect(run.promise).resolves.toEqual([50, 61, 72, 83])
})

it('map with a code-generating function', async () => {
  const run = pool.map<number, number>((item, index) => `${item} ** 2 + ${index}`, [2, 3])
  await expect(run.promise).resolves.toEqual([4, 10])
})

it('map reports progress after each item', async () => {
  const calls: Array<[number, number]> = []
  const run = pool.map<number, number>('item + 1', [1, 2, 3], {
    onProgress: (completed, total) => {
      calls.push([completed, total])
    },
  })
  await expect(run.promise).resolves.toEqual([2, 3, 4])
  expect(calls).toEqual([
    [1, 3],
    [2, 3],
    [3, 3],
  ])
})

it('map on an empty item list resolves immediately (runTasks([]) would hang)', async () => {
  const run = pool.map('item', [])
  expect(run.runId).toBe(-1)
  await expect(run.promise).resolves.toEqual([])
})

it('distributes concurrent tasks across distinct interpreters', async () => {
  // sys.modules persists per interpreter while exec namespaces are fresh, so
  // a marker stashed on sys identifies which interpreter served each task.
  // Four tasks are fanned out synchronously onto 2 warm workers: the first
  // two are guaranteed distinct workers, the rest reuse them — so exactly
  // two distinct markers must come back.
  const code = [
    'import sys',
    'if not hasattr(sys, "_pool_marker"):',
    '    import random',
    '    sys._pool_marker = random.random()',
    'sys._pool_marker',
  ].join('\n')
  const run = pool.map<number>(code, [0, 1, 2, 3])
  const markers = await run.promise
  expect(markers).toHaveLength(4)
  expect(new Set(markers).size).toBe(2)
})

it('recycles the booted interpreter on a size-1 pool instead of re-booting', async () => {
  await withPool(1, async (single) => {
    const [boot] = await single.warmup()
    expect(boot?.bootMs).toBeGreaterThan(0) // the fresh worker paid the boot

    // Task 1 stamps the interpreter; task 2 reads the stamp back — the
    // second task ran on the same recycled interpreter, not a fresh one.
    await single.runPython("import sys\nsys._recycle_probe = 'alive'\nNone")
    await expect(
      single.runPython<string>("import sys\ngetattr(sys, '_recycle_probe', 'missing')"),
    ).resolves.toBe('alive')

    // And no task re-booted it: a ping-backed warmup on the recycled worker
    // reports a bootMs of exactly 0.
    const [recycled] = await single.warmup()
    expect(recycled?.bootMs).toBe(0)
    expect(recycled?.status.booted).toBe(true)
  })
})

it('cancel rejects the map promise with the worker-pool message', async () => {
  const run = pool.map<number>('item', [1, 2, 3, 4, 5, 6, 7, 8])
  run.cancel()
  await expect(run.promise).rejects.toBe('Remaining tasks canceled')
})

// --- runPickled / mapPickled ----------------------------------------------
// Payloads are built and decoded via the pickleCall/unpickle helpers, which
// run cloudpickle on the pool itself — the tests stay independent of any
// host-side pickle implementation.

it('runPickled executes a cloudpickled call and returns pickled result bytes', async () => {
  const payload = await pickleCall(pool, '((lambda a, b: a + b), (20, 22), {})')
  const result = await pool.runPickled(payload)
  expect(result).toBeInstanceOf(ArrayBuffer)
  await expect(unpickle<number>(pool, result)).resolves.toBe(42)
})

it('runPickled rejects with PyodideTaskError carrying traceback and pickled exception', async () => {
  const payload = await pickleCall(pool, '((lambda: [][5]), (), {})')
  const error = await pool.runPickled(payload).catch((err: unknown) => err)
  expect(error).toBeInstanceOf(PyodideTaskError)
  const taskError = error as PyodideTaskError
  expect(taskError.message).toContain('IndexError')
  expect(taskError.pythonTraceback).toContain('IndexError')
  expect(taskError.exceptionPayload).toBeInstanceOf(ArrayBuffer)
  await expect(
    unpickle<string>(pool, taskError.exceptionPayload as ArrayBuffer, 'type(obj).__name__'),
  ).resolves.toBe('IndexError')
})

it('mapPickled preserves input order and reports progress', async () => {
  const payloads = await Promise.all(
    [1, 2, 3].map((n) => pickleCall(pool, `((lambda x: x * x), (${n},), {})`)),
  )
  const calls: Array<[number, number]> = []
  const run = pool.mapPickled(payloads, {
    onProgress: (completed, total) => {
      calls.push([completed, total])
    },
  })
  const results = await run.promise
  const values: number[] = []
  for (const result of results) {
    values.push(await unpickle<number>(pool, result))
  }
  expect(values).toEqual([1, 4, 9])
  expect(calls).toEqual([
    [1, 3],
    [2, 3],
    [3, 3],
  ])
})

it('mapPickled on an empty payload list resolves immediately', async () => {
  const run = pool.mapPickled([])
  expect(run.runId).toBe(-1)
  await expect(run.promise).resolves.toEqual([])
})

it('boots from an explicit pyodideSource (the browser CDN wiring, via file://)', async () => {
  // Browsers pass the jsDelivr pyodide.mjs URL + indexURL; the same code
  // path is exercised here with a file:// module URL into node_modules.
  const pyodideDir = path.join(rootDir, 'node_modules', 'pyodide')
  await withPool(
    1,
    async (sourced) => {
      const [warm] = await sourced.warmup()
      expect(warm?.status.booted).toBe(true)
      await expect(sourced.runPython<number>('21 * 2')).resolves.toBe(42)
    },
    {
      pyodideSource: {
        moduleURL: pathToFileURL(path.join(pyodideDir, 'pyodide.mjs')).href,
        indexURL: pyodideDir,
      },
    },
  )
})
