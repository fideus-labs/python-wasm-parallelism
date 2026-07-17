/**
 * Integration tests for the Pyodide worker message protocol.
 *
 * Bundles src/worker/pyodide-worker.ts exactly like scripts/build.mjs, then
 * drives the real bundle inside Node worker_threads through the `web-worker`
 * polyfill — the same runtime path PyodidePool uses. Tests within this file
 * run sequentially and share one booted worker; boot-sensitive tests
 * (unbooted ping, first-exec boot, warmup ping) are ordered accordingly.
 */
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { build } from 'esbuild'
import Worker from 'web-worker'
import { afterAll, beforeAll, expect, it } from 'vitest'
import type {
  ExecPickledRequest,
  ExecPickledResponse,
  PingStatus,
  WorkerRequest,
  WorkerResponse,
  WorkerSuccess,
} from '../src/worker/pyodide-worker.js'

const rootDir = fileURLToPath(new URL('..', import.meta.url))
const workerFile = path.join(rootDir, 'dist', 'pyodide-worker.js')

let worker: Worker
let nextId = 1

function request<T = unknown>(target: Worker, message: WorkerRequest): Promise<WorkerResponse<T>> {
  return new Promise((resolve) => {
    const onMessage = (event: MessageEvent) => {
      const response = event.data as WorkerResponse<T>
      if (typeof response === 'object' && response !== null && response.id === message.id) {
        target.removeEventListener('message', onMessage)
        resolve(response)
      }
    }
    target.addEventListener('message', onMessage)
    target.postMessage(message)
  })
}

function expectOk<T>(response: WorkerResponse<T>): WorkerSuccess<T> {
  if (!response.ok) throw new Error(`Expected ok response, got error: ${response.error.message}`)
  return response
}

beforeAll(async () => {
  // Same options as scripts/build.mjs for the worker bundle; built directly so
  // the test does not depend on src/index.ts (created by a later task).
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
  worker = new Worker(pathToFileURL(workerFile), { type: 'module' })
})

afterAll(() => {
  worker?.terminate()
})

it('ping without boot reports an unbooted interpreter', async () => {
  const response = expectOk(await request<PingStatus>(worker, { id: nextId++, kind: 'ping' }))
  expect(response.result.booted).toBe(false)
  expect(response.result.loadedPackages).toEqual([])
  expect(response.result.pyodideVersion).toBeNull()
  expect(response.bootMs).toBe(0)
})

it('boots Pyodide lazily on first exec and returns the final expression', async () => {
  const response = expectOk(
    await request<number>(worker, { id: nextId++, kind: 'exec', code: 'sum(range(100))' }),
  )
  expect(response.result).toBe(4950)
  expect(response.bootMs).toBeGreaterThan(0)
  expect(response.execMs).toBeGreaterThanOrEqual(0)
})

it('reuses the booted interpreter on later requests (bootMs === 0)', async () => {
  const response = expectOk(
    await request<number>(worker, { id: nextId++, kind: 'exec', code: '2 ** 10' }),
  )
  expect(response.result).toBe(1024)
  expect(response.bootMs).toBe(0)
})

it('injects message globals into the execution namespace', async () => {
  const response = expectOk(
    await request<number>(worker, {
      id: nextId++,
      kind: 'exec',
      code: "n * config['scale'] + len(items)",
      globals: { n: 21, config: { scale: 2 }, items: [1, 2, 3] },
    }),
  )
  expect(response.result).toBe(45)
})

it('isolates globals between exec requests on the same worker', async () => {
  expectOk(
    await request(worker, { id: nextId++, kind: 'exec', code: "leaked = 'yes'\n'done'" }),
  )
  const response = expectOk(
    await request<boolean>(worker, { id: nextId++, kind: 'exec', code: "'leaked' in globals()" }),
  )
  expect(response.result).toBe(false)
})

it('converts dict results to structured-clone-safe plain objects', async () => {
  const response = expectOk(
    await request(worker, {
      id: nextId++,
      kind: 'exec',
      code: "{'a': 1, 'b': [1, 2, 3], 'c': {'nested': True}, 'd': None}",
    }),
  )
  expect(response.result).toEqual({ a: 1, b: [1, 2, 3], c: { nested: true }, d: undefined })
})

it('returns structured errors with the Python traceback', async () => {
  const response = await request(worker, { id: nextId++, kind: 'exec', code: '1 / 0' })
  expect(response.ok).toBe(false)
  if (response.ok) return
  expect(response.error.message).toContain('ZeroDivisionError')
  expect(response.error.pythonTraceback).toContain('Traceback')
  expect(response.error.pythonTraceback).toContain('ZeroDivisionError')
})

it('answers unconvertible results with an error instead of crashing', async () => {
  const response = await request(worker, {
    id: nextId++,
    kind: 'exec',
    code: 'class Point:\n    pass\n\nPoint()',
  })
  expect(response.ok).toBe(false)
  if (response.ok) return
  expect(response.error.message).toContain('JSON')
  // The worker must survive: same interpreter still answers.
  const followUp = expectOk(
    await request<number>(worker, { id: nextId++, kind: 'exec', code: '1 + 1' }),
  )
  expect(followUp.result).toBe(2)
  expect(followUp.bootMs).toBe(0)
})

it('rejects malformed requests without crashing', async () => {
  const response = await request(worker, { id: nextId++, kind: 'bogus' } as unknown as WorkerRequest)
  expect(response.ok).toBe(false)
  if (response.ok) return
  expect(response.error.message).toContain('Malformed request')
})

it('ping after boot reports status and Pyodide version', async () => {
  const response = expectOk(await request<PingStatus>(worker, { id: nextId++, kind: 'ping' }))
  expect(response.result.booted).toBe(true)
  expect(response.result.pyodideVersion).toBeTypeOf('string')
})

it('ping with boot: true boots a fresh worker (pool warmup path)', async () => {
  const fresh = new Worker(pathToFileURL(workerFile), { type: 'module' })
  try {
    const response = expectOk(
      await request<PingStatus>(fresh, { id: nextId++, kind: 'ping', boot: true }),
    )
    expect(response.result.booted).toBe(true)
    expect(response.result.pyodideVersion).toBeTypeOf('string')
    expect(response.bootMs).toBeGreaterThan(0)
  } finally {
    fresh.terminate()
  }
})

// --- execPickled protocol -------------------------------------------------
// Payloads are built and results decoded through exec requests on the same
// worker (as a list of ints — always structured-clone-safe), so the tests
// stay independent of any host-side pickle implementation.

function requestPickled(target: Worker, message: ExecPickledRequest): Promise<ExecPickledResponse> {
  return new Promise((resolve) => {
    const onMessage = (event: MessageEvent) => {
      const response = event.data as ExecPickledResponse
      if (typeof response === 'object' && response !== null && response.id === message.id) {
        target.removeEventListener('message', onMessage)
        resolve(response)
      }
    }
    target.addEventListener('message', onMessage)
    target.postMessage(message, [message.payload])
  })
}

async function pickleCall(expr: string): Promise<ArrayBuffer> {
  const response = expectOk(
    await request<number[]>(worker, {
      id: nextId++,
      kind: 'exec',
      code: `import cloudpickle\nlist(cloudpickle.dumps(${expr}))`,
      packages: ['cloudpickle'],
    }),
  )
  return new Uint8Array(response.result).buffer
}

async function unpickle<T>(payload: ArrayBuffer, expr = 'obj'): Promise<T> {
  const response = expectOk(
    await request<T>(worker, {
      id: nextId++,
      kind: 'exec',
      code: `import cloudpickle\nobj = cloudpickle.loads(bytes(data))\n${expr}`,
      globals: { data: Array.from(new Uint8Array(payload)) },
      packages: ['cloudpickle'],
    }),
  )
  return response.result
}

it('execPickled runs a cloudpickled call with args and kwargs', async () => {
  const payload = await pickleCall("(sorted, ([3, 1, 2],), {'reverse': True})")
  const response = await requestPickled(worker, {
    id: nextId++,
    kind: 'execPickled',
    payload,
    packages: [],
    wheels: [],
  })
  if (!response.ok) throw new Error(`Expected ok response, got: ${response.error.message}`)
  expect(response.payload).toBeInstanceOf(ArrayBuffer)
  expect(response.bootMs).toBe(0) // interpreter reused from earlier tests
  await expect(unpickle(response.payload)).resolves.toEqual([3, 2, 1])
})

it('execPickled ships lambdas by value (cloudpickle, not pickle)', async () => {
  const payload = await pickleCall('((lambda a, b: a * b), (6, 7), {})')
  const response = await requestPickled(worker, {
    id: nextId++,
    kind: 'execPickled',
    payload,
    packages: [],
    wheels: [],
  })
  if (!response.ok) throw new Error(`Expected ok response, got: ${response.error.message}`)
  await expect(unpickle(response.payload)).resolves.toBe(42)
})

it('execPickled failures carry the traceback and a pickled exception', async () => {
  const payload = await pickleCall('((lambda: 1 / 0), (), {})')
  const response = await requestPickled(worker, {
    id: nextId++,
    kind: 'execPickled',
    payload,
    packages: [],
    wheels: [],
  })
  expect(response.ok).toBe(false)
  if (response.ok) return
  expect(response.error.message).toContain('ZeroDivisionError')
  expect(response.error.pythonTraceback).toContain('Traceback')
  expect(response.error.pythonTraceback).toContain('ZeroDivisionError')
  expect(response.error.exceptionPayload).toBeInstanceOf(ArrayBuffer)
  const typeName = await unpickle<string>(
    response.error.exceptionPayload as ArrayBuffer,
    'type(obj).__name__',
  )
  expect(typeName).toBe('ZeroDivisionError')
  // The worker must survive a failed pickled task: same interpreter answers.
  const followUp = expectOk(
    await request<number>(worker, { id: nextId++, kind: 'exec', code: '1 + 1' }),
  )
  expect(followUp.result).toBe(2)
  expect(followUp.bootMs).toBe(0)
})

it('rejects execPickled requests without a payload buffer', async () => {
  const response = await request(worker, {
    id: nextId++,
    kind: 'execPickled',
    packages: [],
    wheels: [],
  } as unknown as WorkerRequest)
  expect(response.ok).toBe(false)
  if (response.ok) return
  expect(response.error.message).toContain('Malformed request')
})
