/**
 * Pyodide worker entry: one-request/one-response message protocol.
 *
 * Each message is a complete exchange: the pool-side task posts one request
 * and waits for exactly one response with a matching `id` (the worker-pool
 * task contract re-assigns `onmessage` per task, so the worker must never
 * post unsolicited messages).
 *
 * The Pyodide interpreter lives in module scope: the first request pays the
 * multi-second `loadPyodide()` boot, and a worker recycled by the pool reuses
 * the booted interpreter (and its imported modules) on every later request.
 *
 * The same ESM bundle (dist/pyodide-worker.js, `pyodide` kept external) runs
 * in browser Web Workers and in Node worker_threads via the `web-worker`
 * polyfill, which emulates WorkerGlobalScope (`self`, `postMessage`,
 * `addEventListener`) around this module. In Node, `loadPyodide()` resolves
 * the WASM assets from node_modules/pyodide.
 */
import { loadPyodide } from 'pyodide'
import type { PyodideAPI } from 'pyodide'

/** Run Python source; the value of the final expression becomes `result`. */
export interface ExecRequest {
  id: number
  kind: 'exec'
  code: string
  /**
   * Structured-clone-safe values deep-converted (`pyodide.toPy`) into the
   * execution namespace before the code runs.
   */
  globals?: Record<string, unknown>
  /**
   * Packages to ensure are loaded before running: `pyodide.loadPackage`
   * (distribution) first, micropip (PyPI wheels) as fallback.
   */
  packages?: string[]
}

/** Interpreter status probe; `boot: true` forces the boot (pool warmup). */
export interface PingRequest {
  id: number
  kind: 'ping'
  boot?: boolean
}

export type WorkerRequest = ExecRequest | PingRequest

/** `result` of a ping — interpreter status after the ping was handled. */
export interface PingStatus {
  booted: boolean
  loadedPackages: string[]
  pyodideVersion: string | null
}

export interface WorkerErrorInfo {
  message: string
  stack?: string
  /** Full Python traceback when the failure came from Python code. */
  pythonTraceback?: string
}

export interface WorkerSuccess<T = unknown> {
  id: number
  ok: true
  result: T
  /** Time booting Pyodide for this request; 0 when the interpreter was reused. */
  bootMs: number
  /** Time inside runPythonAsync (excludes boot and package loading); 0 for pings. */
  execMs: number
}

export interface WorkerFailure {
  id: number
  ok: false
  error: WorkerErrorInfo
}

export type WorkerResponse<T = unknown> = WorkerSuccess<T> | WorkerFailure

type PyProxy = InstanceType<PyodideAPI['ffi']['PyProxy']>
type PyDictProxy = PyProxy & {
  get(key: string): any
  set(key: string, value: unknown): void
}

// Narrow view of the worker global scope; keeps us independent of the
// DOM-vs-WebWorker lib skew and matches web-worker's Node shim exactly.
interface WorkerScope {
  addEventListener(type: 'message', listener: (event: { data: unknown }) => void): void
  postMessage(message: unknown): void
}
const scope = globalThis as unknown as WorkerScope

const now = (): number => performance.now()

let pyodidePromise: Promise<PyodideAPI> | null = null

async function ensurePyodide(): Promise<{ py: PyodideAPI; bootMs: number }> {
  if (pyodidePromise !== null) {
    return { py: await pyodidePromise, bootMs: 0 }
  }
  const started = now()
  pyodidePromise = loadPyodide()
  try {
    const py = await pyodidePromise
    return { py, bootMs: now() - started }
  } catch (err) {
    pyodidePromise = null // allow the next request to retry the boot
    throw err
  }
}

async function ensurePackages(py: PyodideAPI, packages: string[]): Promise<void> {
  const missing = packages.filter((name) => !(name in py.loadedPackages))
  if (missing.length === 0) return

  let distributionError: unknown = null
  try {
    await py.loadPackage(missing, { messageCallback: () => {} })
  } catch (err) {
    distributionError = err
  }

  const remaining = missing.filter((name) => !(name in py.loadedPackages))
  if (remaining.length === 0) return

  try {
    await py.loadPackage('micropip', { messageCallback: () => {} })
    const micropip = py.pyimport('micropip') as {
      install(requirements: unknown): Promise<void>
      destroy(): void
    }
    const requirements = py.toPy(remaining) as PyProxy
    try {
      await micropip.install(requirements)
    } finally {
      requirements.destroy()
      micropip.destroy()
    }
  } catch (micropipError) {
    const detail =
      distributionError === null
        ? ''
        : `; pyodide.loadPackage first failed with: ${errorMessage(distributionError)}`
    throw new Error(
      `Failed to load package(s) [${remaining.join(', ')}] via pyodide.loadPackage and micropip: ` +
        `${errorMessage(micropipError)}${detail}`,
    )
  }
}

/**
 * Convert a runPythonAsync result to a structured-clone-safe value.
 * JS pass-through -> toJs (dicts become plain objects, no PyProxies) ->
 * JSON round-trip; a failure of all three propagates to dispatch(), which
 * answers with an error response.
 */
function toCloneSafe(py: PyodideAPI, value: unknown): unknown {
  if (!(value instanceof py.ffi.PyProxy)) {
    return value // None -> undefined, int -> number, str -> string, ...
  }
  try {
    try {
      return value.toJs({ dict_converter: Object.fromEntries, create_pyproxies: false })
    } catch {
      const jsonModule = py.pyimport('json') as {
        dumps(value: unknown): string
        destroy(): void
      }
      try {
        return JSON.parse(jsonModule.dumps(value)) as unknown
      } finally {
        jsonModule.destroy()
      }
    }
  } finally {
    value.destroy()
  }
}

async function handleExec(request: ExecRequest): Promise<WorkerResponse> {
  const { py, bootMs } = await ensurePyodide()
  if (request.packages !== undefined && request.packages.length > 0) {
    await ensurePackages(py, request.packages)
  }
  // Fresh namespace per exec: a recycled worker keeps its interpreter (and
  // sys.modules) warm, but tasks must not see each other's globals.
  const namespace = (py.globals as PyDictProxy).get('dict')() as PyDictProxy
  try {
    if (request.globals !== undefined) {
      for (const [key, value] of Object.entries(request.globals)) {
        const converted: unknown = py.toPy(value)
        namespace.set(key, converted)
        if (converted instanceof py.ffi.PyProxy) converted.destroy()
      }
    }
    const started = now()
    const raw: unknown = await py.runPythonAsync(request.code, { globals: namespace })
    const execMs = now() - started
    return { id: request.id, ok: true, result: toCloneSafe(py, raw), bootMs, execMs }
  } finally {
    namespace.destroy()
  }
}

async function handlePing(request: PingRequest): Promise<WorkerResponse<PingStatus>> {
  let bootMs = 0
  if (request.boot === true) {
    ;({ bootMs } = await ensurePyodide())
  }
  const py = pyodidePromise === null ? null : await pyodidePromise
  return {
    id: request.id,
    ok: true,
    result: {
      booted: py !== null,
      loadedPackages: py === null ? [] : Object.keys(py.loadedPackages).sort(),
      pyodideVersion: py === null ? null : py.version,
    },
    bootMs,
    execMs: 0,
  }
}

function errorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err)
}

function toErrorInfo(err: unknown): WorkerErrorInfo {
  if (err instanceof Error) {
    const info: WorkerErrorInfo = { message: err.message }
    if (typeof err.stack === 'string') info.stack = err.stack
    // Pyodide's PythonError carries the full traceback in `message` and the
    // Python exception class name in `type`. Duck-typed so it works whether
    // or not the interpreter finished booting.
    const type = (err as { type?: unknown }).type
    if (err.constructor.name === 'PythonError' && typeof type === 'string') {
      info.pythonTraceback = err.message
      const lines = err.message.trimEnd().split('\n')
      info.message = lines[lines.length - 1]?.trim() ?? err.message
    }
    return info
  }
  return { message: errorMessage(err) }
}

function describeValue(value: unknown): string {
  try {
    return JSON.stringify(value) ?? String(value)
  } catch {
    return String(value)
  }
}

function isRequest(data: unknown): data is WorkerRequest {
  if (typeof data !== 'object' || data === null) return false
  const record = data as Record<string, unknown>
  if (typeof record.id !== 'number') return false
  if (record.kind === 'exec') return typeof record.code === 'string'
  return record.kind === 'ping'
}

function post(response: WorkerResponse): void {
  try {
    scope.postMessage(response)
  } catch (err) {
    // The converted result still contained something structured clone
    // rejects; better an explicit error than a corrupted payload.
    const failure: WorkerFailure = {
      id: response.id,
      ok: false,
      error: { message: `Response could not be structured-cloned: ${errorMessage(err)}` },
    }
    scope.postMessage(failure)
  }
}

async function dispatch(data: unknown): Promise<void> {
  if (!isRequest(data)) {
    const id = (data as { id?: unknown } | null)?.id
    post({
      id: typeof id === 'number' ? id : -1,
      ok: false,
      error: {
        message: `Malformed request (expected { id, kind: 'exec' | 'ping' }): ${describeValue(data)}`,
      },
    })
    return
  }
  try {
    post(data.kind === 'exec' ? await handleExec(data) : await handlePing(data))
  } catch (err) {
    post({ id: data.id, ok: false, error: toErrorInfo(err) })
  }
}

scope.addEventListener('message', (event) => {
  void dispatch(event.data)
})
