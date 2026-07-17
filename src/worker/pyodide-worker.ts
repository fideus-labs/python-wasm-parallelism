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

/**
 * Run one cloudpickled call: `payload` is `cloudpickle.dumps((func, args,
 * kwargs))`. The call executes through a fixed Python shim (unpickle ->
 * call -> repickle) and the result posts back as cloudpickled bytes,
 * bypassing `exec`'s structured-clone conversion chain so arbitrary
 * picklable Python objects round-trip. Payload buffers ride the postMessage
 * transfer list in both directions (moved, not copied).
 */
export interface ExecPickledRequest {
  id: number
  kind: 'execPickled'
  /** cloudpickle.dumps((func, args, kwargs)) — detached from the sender. */
  payload: ArrayBuffer
  /**
   * Packages mirrored from the driver instance, ensured loaded before the
   * call runs: `pyodide.loadPackage` (distribution) first, micropip (PyPI
   * wheels) as fallback.
   */
  packages: string[]
  /**
   * micropip targets (PyPI names or wheel URLs) mirrored from the driver.
   * A module-scope record of installed targets makes replaying the same
   * snapshot on every message cheap and idempotent.
   */
  wheels: string[]
}

/** Interpreter status probe; `boot: true` forces the boot (pool warmup). */
export interface PingRequest {
  id: number
  kind: 'ping'
  boot?: boolean
}

export type WorkerRequest = ExecRequest | ExecPickledRequest | PingRequest

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
  /**
   * `cloudpickle.dumps(exception)` when an `execPickled` task raised and the
   * exception object was picklable — lets the driver re-raise the original
   * exception. Transferred, not cloned.
   */
  exceptionPayload?: ArrayBuffer
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

/** Success response to an {@link ExecPickledRequest}. */
export interface ExecPickledSuccess {
  id: number
  ok: true
  /** cloudpickle.dumps(result) — transferred, not cloned. */
  payload: ArrayBuffer
  /** Time booting Pyodide for this request; 0 when the interpreter was reused. */
  bootMs: number
  /** Time unpickling + executing the call (excludes boot and package loading). */
  execMs: number
}

export type ExecPickledResponse = ExecPickledSuccess | WorkerFailure

type PyProxy = InstanceType<PyodideAPI['ffi']['PyProxy']>
type PyDictProxy = PyProxy & {
  get(key: string): any
  set(key: string, value: unknown): void
}

// Narrow view of the worker global scope; keeps us independent of the
// DOM-vs-WebWorker lib skew and matches web-worker's Node shim exactly.
interface WorkerScope {
  addEventListener(type: 'message', listener: (event: { data: unknown }) => void): void
  postMessage(message: unknown, transfer?: Transferable[]): void
}
const scope = globalThis as unknown as WorkerScope

const now = (): number => performance.now()

let pyodidePromise: Promise<PyodideAPI> | null = null

async function ensurePyodide(): Promise<{ py: PyodideAPI; bootMs: number }> {
  if (pyodidePromise !== null) {
    return { py: await pyodidePromise, bootMs: 0 }
  }
  const started = now()
  // Node worker_threads have no process.stdout.fd, so Pyodide's default
  // Node stdout/stderr device throws on every Python-level write (e.g.
  // micropip's internal "Loading ..." messages while mirroring packages).
  // console.log/error are wired up correctly in worker_threads and browser
  // workers alike.
  pyodidePromise = loadPyodide({
    stdout: (line) => console.log(line),
    stderr: (line) => console.error(line),
  })
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
 * micropip targets already installed by this worker. `pyodide.loadedPackages`
 * records installs by package name only, so URL-form wheel targets need
 * their own record for replays to stay idempotent — the driver re-sends its
 * full package snapshot with every execPickled message.
 */
const installedWheels = new Set<string>()

async function ensureWheels(py: PyodideAPI, wheels: string[]): Promise<void> {
  const missing = wheels.filter(
    (target) => !installedWheels.has(target) && !(target in py.loadedPackages),
  )
  if (missing.length === 0) return
  await py.loadPackage('micropip', { messageCallback: () => {} })
  const micropip = py.pyimport('micropip') as {
    install(requirements: unknown): Promise<void>
    destroy(): void
  }
  const requirements = py.toPy(missing) as PyProxy
  try {
    await micropip.install(requirements)
  } catch (err) {
    throw new Error(
      `Failed to install wheel target(s) [${missing.join(', ')}] via micropip: ${errorMessage(err)}`,
    )
  } finally {
    requirements.destroy()
    micropip.destroy()
  }
  for (const target of missing) installedWheels.add(target)
}

/**
 * Fixed Python shim for execPickled: unpickle -> call -> repickle, defined
 * once per interpreter so no per-task Python source is generated. Failures
 * inside the call are caught in Python so the full traceback survives and
 * the exception object itself can be cloudpickled for driver-side
 * re-raising. Returns [ok, resultBytes, formattedTraceback, exceptionBytes];
 * byte fields are JS Uint8Arrays built Python-side (JsBuffer.assign), so
 * their buffers are plain ArrayBuffers that are safe to transfer.
 */
const RUN_PICKLED_SOURCE = `
def _make_run_pickled():
    import traceback

    import cloudpickle
    from js import Uint8Array

    def _to_js_bytes(data):
        buffer = Uint8Array.new(len(data))
        buffer.assign(data)
        return buffer

    def _run_pickled(payload):
        try:
            func, args, kwargs = cloudpickle.loads(payload.to_bytes())
            result = func(*args, **(kwargs or {}))
            return [True, _to_js_bytes(cloudpickle.dumps(result)), None, None]
        except BaseException as exc:
            formatted = "".join(traceback.format_exception(exc))
            pickled_exc = None
            try:
                pickled_exc = _to_js_bytes(cloudpickle.dumps(exc))
            except BaseException:
                pass
            return [False, None, formatted, pickled_exc]

    return _run_pickled


_make_run_pickled()
`

type PyListProxy = PyProxy & { get(index: number): unknown }
type RunPickledShim = (payload: Uint8Array) => PyListProxy

let runPickledShim: RunPickledShim | null = null

async function ensureRunPickledShim(py: PyodideAPI): Promise<RunPickledShim> {
  if (runPickledShim !== null) return runPickledShim
  await ensurePackages(py, ['cloudpickle'])
  const namespace = (py.globals as PyDictProxy).get('dict')() as PyDictProxy
  try {
    // The shim keeps its own Python reference to the namespace; destroying
    // the proxy only releases the JS handle.
    runPickledShim = py.runPython(RUN_PICKLED_SOURCE, { globals: namespace }) as RunPickledShim
  } finally {
    namespace.destroy()
  }
  return runPickledShim
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

async function handleExecPickled(request: ExecPickledRequest): Promise<ExecPickledResponse> {
  const { py, bootMs } = await ensurePyodide()
  if (request.packages.length > 0) {
    await ensurePackages(py, request.packages)
  }
  if (request.wheels.length > 0) {
    await ensureWheels(py, request.wheels)
  }
  const shim = await ensureRunPickledShim(py)
  const started = now()
  const outcome = shim(new Uint8Array(request.payload))
  const execMs = now() - started
  try {
    if (outcome.get(0) === true) {
      const result = outcome.get(1) as Uint8Array
      return { id: request.id, ok: true, payload: result.buffer as ArrayBuffer, bootMs, execMs }
    }
    const formatted = outcome.get(2) as string
    const pickledExc = outcome.get(3) as Uint8Array | null | undefined
    const lines = formatted.trimEnd().split('\n')
    const error: WorkerErrorInfo = {
      message: lines[lines.length - 1]?.trim() ?? 'execPickled task failed',
      pythonTraceback: formatted,
    }
    if (pickledExc !== null && pickledExc !== undefined) {
      error.exceptionPayload = pickledExc.buffer as ArrayBuffer
    }
    return { id: request.id, ok: false, error }
  } finally {
    outcome.destroy()
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
  if (record.kind === 'execPickled') {
    return (
      record.payload instanceof ArrayBuffer &&
      Array.isArray(record.packages) &&
      Array.isArray(record.wheels)
    )
  }
  return record.kind === 'ping'
}

/** Buffers moved (not copied) with an execPickled response. */
function execPickledTransfer(response: ExecPickledResponse): Transferable[] {
  if (response.ok) return [response.payload]
  return response.error.exceptionPayload === undefined ? [] : [response.error.exceptionPayload]
}

function post(response: WorkerResponse | ExecPickledResponse, transfer?: Transferable[]): void {
  try {
    scope.postMessage(response, transfer)
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
        message: `Malformed request (expected { id, kind: 'exec' | 'execPickled' | 'ping' }): ${describeValue(data)}`,
      },
    })
    return
  }
  try {
    if (data.kind === 'exec') {
      post(await handleExec(data))
    } else if (data.kind === 'execPickled') {
      const response = await handleExecPickled(data)
      post(response, execPickledTransfer(response))
    } else {
      post(await handlePing(data))
    }
  } catch (err) {
    post({ id: data.id, ok: false, error: toErrorInfo(err) })
  }
}

scope.addEventListener('message', (event) => {
  void dispatch(event.data)
})
