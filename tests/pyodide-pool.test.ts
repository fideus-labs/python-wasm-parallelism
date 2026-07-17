/**
 * Integration tests for PyodidePool over the real worker bundle.
 *
 * Builds dist/pyodide-worker.js exactly like scripts/build.mjs, then drives
 * a real 2-worker pool in Node — the environment-aware factory resolves the
 * `web-worker` polyfill because Node has no global Worker. One pool is
 * shared across tests to amortize interpreter boots; the warmup test runs
 * first because it asserts on fresh (bootMs > 0) workers.
 */
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { build } from 'esbuild'
import { afterAll, beforeAll, expect, it } from 'vitest'
import { PyodidePool, PyodideTaskError } from '../src/index.js'

const rootDir = fileURLToPath(new URL('..', import.meta.url))
const workerFile = path.join(rootDir, 'dist', 'pyodide-worker.js')

let pool: PyodidePool

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
})

afterAll(() => {
  pool?.terminate()
})

it('rejects invalid pool sizes', () => {
  expect(() => new PyodidePool({ poolSize: 0 })).toThrow(RangeError)
  expect(() => new PyodidePool({ poolSize: 1.5 })).toThrow(RangeError)
})

it('warmup boots poolSize interpreters in parallel', async () => {
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

it('map with a string template injects item and index, ordered results', async () => {
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

it('cancel rejects the map promise with the worker-pool message', async () => {
  const run = pool.map<number>('item', [1, 2, 3, 4, 5, 6, 7, 8])
  run.cancel()
  await expect(run.promise).rejects.toBe('Remaining tasks canceled')
})

// --- runPickled / mapPickled ----------------------------------------------
// Payloads are built and results decoded via runPython on the same pool (as
// lists of ints — always structured-clone-safe), so the tests stay
// independent of any host-side pickle implementation.

async function pickleCall(expr: string): Promise<ArrayBuffer> {
  const bytes = await pool.runPython<number[]>(
    `import cloudpickle\nlist(cloudpickle.dumps(${expr}))`,
    { packages: ['cloudpickle'] },
  )
  return new Uint8Array(bytes).buffer
}

async function unpickle<T>(payload: ArrayBuffer): Promise<T> {
  return pool.runPython<T>('import cloudpickle\ncloudpickle.loads(bytes(data))', {
    globals: { data: Array.from(new Uint8Array(payload)) },
    packages: ['cloudpickle'],
  })
}

it('runPickled executes a cloudpickled call and returns pickled result bytes', async () => {
  const payload = await pickleCall('((lambda a, b: a + b), (20, 22), {})')
  const result = await pool.runPickled(payload)
  expect(result).toBeInstanceOf(ArrayBuffer)
  await expect(unpickle<number>(result)).resolves.toBe(42)
})

it('runPickled rejects with PyodideTaskError carrying traceback and pickled exception', async () => {
  const payload = await pickleCall('((lambda: [][5]), (), {})')
  const error = await pool.runPickled(payload).catch((err: unknown) => err)
  expect(error).toBeInstanceOf(PyodideTaskError)
  const taskError = error as PyodideTaskError
  expect(taskError.message).toContain('IndexError')
  expect(taskError.pythonTraceback).toContain('IndexError')
  expect(taskError.exceptionPayload).toBeInstanceOf(ArrayBuffer)
})

it('mapPickled preserves input order and reports progress', async () => {
  const payloads = await Promise.all(
    [1, 2, 3].map((n) => pickleCall(`((lambda x: x * x), (${n},), {})`)),
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
    values.push(await unpickle<number>(result))
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
