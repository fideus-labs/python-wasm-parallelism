/**
 * PyodidePool: parallel Python execution on a pool of Pyodide Web Workers.
 *
 * A thin wrapper around `WorkerPool` from @fideus-labs/worker-pool — queuing,
 * bounded concurrency, LIFO worker recycling, progress reporting, and
 * cancellation all come from the pool; this class adds the one-request/
 * one-response Pyodide message protocol on top.
 *
 * Two worker-pool behaviors (verified against worker-pool 1.0.0) shape the
 * implementation:
 *
 * - A task function that REJECTS permanently removes its worker slot from
 *   the pool (the catch path never pushes the worker back). Task functions
 *   here therefore always resolve with the raw `WorkerResponse`; failures
 *   are unwrapped into thrown `PyodideTaskError`s only after the pool has
 *   recycled the worker.
 * - `runTasks([])` never settles (resolution is driven by task completion),
 *   so `map()` short-circuits on empty input.
 */
import { WorkerPool } from '@fideus-labs/worker-pool'
import type { WorkerPoolProgressCallback, WorkerPoolTask } from '@fideus-labs/worker-pool'
import type {
  ExecPickledRequest,
  ExecPickledResponse,
  ExecRequest,
  PingRequest,
  PingStatus,
  PyodideSource,
  WorkerErrorInfo,
  WorkerFailure,
  WorkerRequest,
  WorkerResponse,
} from '../worker/pyodide-worker.js'

/** Options for {@link PyodidePool}. */
export interface PyodidePoolOptions {
  /** Maximum number of concurrent workers — one Pyodide interpreter each. */
  poolSize: number
  /**
   * URL of the built worker bundle (`dist/pyodide-worker.js`). Defaults to
   * `pyodide-worker.js` next to this module, which is correct for the built
   * `dist/index.js`. When running from source (tsx, vitest), pass an
   * absolute `file://` URL instead: the `web-worker` polyfill resolves
   * relative paths against `process.cwd()`, not the importing module.
   */
  workerUrl?: string | URL
  /**
   * Packages ensured loaded (Pyodide distribution first, micropip fallback)
   * before every exec, and preloaded during {@link PyodidePool.warmup}.
   */
  packages?: string[]
  /**
   * Where workers obtain `loadPyodide` and the runtime assets, attached to
   * every request (a worker boots on its first). Omitted, workers import the
   * bare `pyodide` package — correct in Node, where it resolves from
   * node_modules. Browser embedders must pass a CDN (or same-origin) module
   * URL and matching `indexURL` instead.
   */
  pyodideSource?: PyodideSource
}

/** Options for {@link PyodidePool.runPython}. */
export interface RunPythonOptions {
  /** Structured-clone-safe values injected into the execution namespace. */
  globals?: Record<string, unknown>
  /** Extra packages for this call, merged with the pool-level `packages`. */
  packages?: string[]
}

/** Options for {@link PyodidePool.map}. */
export interface MapOptions {
  /** Invoked after each item completes with (completedTasks, totalTasks). */
  onProgress?: WorkerPoolProgressCallback
  /** Extra packages for this run, merged with the pool-level `packages`. */
  packages?: string[]
}

/** Options for {@link PyodidePool.runPickled}. */
export interface RunPickledOptions {
  /**
   * Pyodide-distribution packages ensured loaded on the worker before the
   * call executes, merged with the pool-level `packages`.
   */
  packages?: string[]
  /** micropip targets (PyPI names or wheel URLs) ensured installed first. */
  wheels?: string[]
}

/** Options for {@link PyodidePool.mapPickled}. */
export interface MapPickledOptions extends RunPickledOptions {
  /** Invoked after each payload completes with (completedTasks, totalTasks). */
  onProgress?: WorkerPoolProgressCallback
}

/** Handle returned by {@link PyodidePool.map}. */
export interface PyodideMapRun<T> {
  /** Resolves with per-item results in input order. */
  promise: Promise<T[]>
  /** worker-pool run id; -1 when `items` was empty (nothing to cancel). */
  runId: number
  /**
   * Cancel the items not yet started; `promise` then rejects with the
   * worker-pool string `'Remaining tasks canceled'`. In-flight items finish.
   */
  cancel: () => void
}

/** Per-worker outcome of {@link PyodidePool.warmup}. */
export interface WarmupResult {
  /** Time this worker spent booting Pyodide (0 if it was already booted). */
  bootMs: number
  /** Interpreter status after boot (and package preload, if configured). */
  status: PingStatus
}

/** A Python (or worker-level) failure, re-thrown on the pool side. */
export class PyodideTaskError extends Error {
  /** Full Python traceback when the failure came from Python code. */
  readonly pythonTraceback?: string
  /** JS stack from inside the worker, when available. */
  readonly workerStack?: string
  /**
   * `cloudpickle.dumps(exception)` from a failed `execPickled` task, when
   * the exception object was picklable — lets a Python driver re-raise the
   * original exception.
   */
  readonly exceptionPayload?: ArrayBuffer

  constructor(info: WorkerErrorInfo) {
    super(info.message)
    this.name = 'PyodideTaskError'
    this.pythonTraceback = info.pythonTraceback
    this.workerStack = info.stack
    this.exceptionPayload = info.exceptionPayload
  }
}

type WorkerCtor = new (
  url: string | URL,
  options?: { type?: 'classic' | 'module'; name?: string },
) => Worker

let workerCtorPromise: Promise<WorkerCtor> | null = null

/**
 * Browsers (and Deno) expose `Worker` globally; Node does not. Fall back to
 * the `web-worker` polyfill over worker_threads, imported dynamically so
 * this module stays platform-neutral and browser bundles never pull in the
 * polyfill.
 */
function resolveWorkerCtor(): Promise<WorkerCtor> {
  if (workerCtorPromise === null) {
    workerCtorPromise = (async () => {
      const globalWorker = (globalThis as { Worker?: unknown }).Worker
      if (typeof globalWorker === 'function') {
        return globalWorker as WorkerCtor
      }
      const polyfill = await import('web-worker')
      return polyfill.default
    })()
  }
  return workerCtorPromise
}

function unwrap<T>(response: WorkerResponse<T>): T {
  if (!response.ok) throw new PyodideTaskError(response.error)
  return response.result
}

function unwrapPickled(response: ExecPickledResponse): ArrayBuffer {
  if (!response.ok) throw new PyodideTaskError(response.error)
  return response.payload
}

function describeErrorEvent(event: ErrorEvent): string {
  if (typeof event.message === 'string' && event.message !== '') return event.message
  const error: unknown = event.error
  if (error instanceof Error) return error.message
  return String(error ?? 'unknown error')
}

/**
 * A pool of Web Workers, each running its own Pyodide interpreter, for
 * executing CPU-bound Python in parallel. Workers boot lazily on first use
 * (or eagerly via {@link warmup}) and recycled workers keep their booted
 * interpreter, amortizing the multi-second Pyodide startup across tasks.
 */
export class PyodidePool {
  readonly poolSize: number
  private readonly pool: WorkerPool
  private readonly workerUrl: string | URL
  private readonly packages: string[]
  private readonly pyodideSource: PyodideSource | undefined
  /** Workers that fired an 'error' event; replaced instead of reused. */
  private readonly deadWorkers = new WeakSet<Worker>()
  private nextMessageId = 1

  constructor(options: PyodidePoolOptions) {
    if (!Number.isInteger(options.poolSize) || options.poolSize < 1) {
      throw new RangeError(`poolSize must be a positive integer, got ${String(options.poolSize)}`)
    }
    this.poolSize = options.poolSize
    // @vite-ignore: the default resolves next to the BUILT dist/index.js at
    // runtime; browser bundles always pass an explicit workerUrl instead.
    this.workerUrl = options.workerUrl ?? new URL(/* @vite-ignore */ 'pyodide-worker.js', import.meta.url)
    this.packages = options.packages ?? []
    this.pyodideSource = options.pyodideSource
    this.pool = new WorkerPool(options.poolSize)
  }

  /** Run one Python snippet; resolves with the final expression's value. */
  async runPython<T = unknown>(code: string, options: RunPythonOptions = {}): Promise<T> {
    const request = this.execRequest(code, options.globals, options.packages)
    const { promise } = this.pool.runTasks([this.protocolTask<T>(request)])
    const [response] = await promise
    if (response === undefined) {
      throw new Error('worker-pool resolved without a response')
    }
    return unwrap(response)
  }

  /**
   * Run one Python task per item with bounded parallelism.
   *
   * With a string `code` template, each item is injected into the execution
   * namespace as `item` (and its position as `index`) — items must be
   * structured-clone-safe. With a function, `code(item, index)` produces the
   * Python source per item and nothing is injected.
   */
  map<T = unknown, I = unknown>(
    code: string | ((item: I, index: number) => string),
    items: readonly I[],
    options: MapOptions = {},
  ): PyodideMapRun<T> {
    if (items.length === 0) {
      // worker-pool's runTasks([]) never settles — resolution is driven by
      // task completion — so an empty run must short-circuit here.
      return { promise: Promise.resolve([]), runId: -1, cancel: () => {} }
    }
    const tasks = items.map((item, index) => {
      const request =
        typeof code === 'function'
          ? this.execRequest(code(item, index), undefined, options.packages)
          : this.execRequest(code, { item, index }, options.packages)
      return this.protocolTask<T>(request)
    })
    const { promise, runId } = this.pool.runTasks(tasks, options.onProgress ?? null)
    return {
      promise: promise.then((responses) => responses.map((response) => unwrap(response))),
      runId,
      cancel: () => {
        this.pool.cancel(runId)
      },
    }
  }

  /**
   * Run one cloudpickled call — `payload` is `cloudpickle.dumps((func,
   * args, kwargs))` — and resolve with the cloudpickled result bytes.
   * The payload buffer is posted as a transferable (zero-copy), so the
   * caller's ArrayBuffer is detached after this call.
   */
  async runPickled(payload: ArrayBuffer, options: RunPickledOptions = {}): Promise<ArrayBuffer> {
    const request = this.execPickledRequest(payload, options)
    const { promise } = this.pool.runTasks([this.pickledTask(request)])
    const [response] = await promise
    if (response === undefined) {
      throw new Error('worker-pool resolved without a response')
    }
    return unwrapPickled(response)
  }

  /**
   * Run one cloudpickled call per payload with bounded parallelism. Same
   * progress-reporting and `cancel(runId)` contract as {@link map}. Payload
   * buffers are posted as transferables (zero-copy), so they are detached
   * once dispatched — pass each buffer at most once.
   */
  mapPickled(
    payloads: readonly ArrayBuffer[],
    options: MapPickledOptions = {},
  ): PyodideMapRun<ArrayBuffer> {
    if (payloads.length === 0) {
      // Same worker-pool runTasks([]) hang as map() — short-circuit.
      return { promise: Promise.resolve([]), runId: -1, cancel: () => {} }
    }
    const tasks = payloads.map((payload) =>
      this.pickledTask(this.execPickledRequest(payload, options)),
    )
    const { promise, runId } = this.pool.runTasks(tasks, options.onProgress ?? null)
    return {
      promise: promise.then((responses) => responses.map((response) => unwrapPickled(response))),
      runId,
      cancel: () => {
        this.pool.cancel(runId)
      },
    }
  }

  /**
   * Boot `poolSize` interpreters in parallel (and preload the pool-level
   * `packages`) so later timing-sensitive work hits only warm workers.
   * The synchronous task fan-out in worker-pool guarantees each warmup task
   * lands on a distinct worker slot.
   */
  async warmup(): Promise<WarmupResult[]> {
    const tasks = Array.from(
      { length: this.poolSize },
      (): WorkerPoolTask<WorkerResponse<PingStatus>> => {
        if (this.packages.length === 0) {
          const request = this.pingRequest(true)
          return async (worker) => {
            const { worker: target, response } = await this.exchange<WorkerResponse<PingStatus>>(
              worker,
              request,
            )
            return { worker: target, result: response }
          }
        }
        // With packages configured, warm up via a no-op exec (which boots
        // AND preloads), then probe the resulting status with a plain ping.
        const execRequest = this.execRequest('None', undefined, undefined)
        const pingRequest = this.pingRequest(false)
        return async (worker) => {
          const exec = await this.exchange<WorkerResponse<unknown>>(worker, execRequest)
          if (!exec.response.ok) {
            return { worker: exec.worker, result: exec.response }
          }
          const ping = await this.exchange<WorkerResponse<PingStatus>>(exec.worker, pingRequest)
          const result = ping.response.ok
            ? { ...ping.response, bootMs: exec.response.bootMs }
            : ping.response
          return { worker: ping.worker, result }
        }
      },
    )
    const { promise } = this.pool.runTasks(tasks)
    const responses = await promise
    return responses.map((response) => {
      if (!response.ok) throw new PyodideTaskError(response.error)
      return { bootMs: response.bootMs, status: response.result }
    })
  }

  /**
   * Terminate all idle workers. In-flight tasks keep their workers until
   * completion (the pool then recycles fresh slots). The pool stays usable —
   * the next task boots a fresh interpreter.
   */
  terminate(): void {
    this.pool.terminateWorkers()
  }

  private execRequest(
    code: string,
    globals: Record<string, unknown> | undefined,
    packages: string[] | undefined,
  ): ExecRequest {
    const request: ExecRequest = { id: this.nextMessageId++, kind: 'exec', code }
    if (globals !== undefined) request.globals = globals
    const merged = [...new Set([...this.packages, ...(packages ?? [])])]
    if (merged.length > 0) request.packages = merged
    if (this.pyodideSource !== undefined) request.pyodide = this.pyodideSource
    return request
  }

  private execPickledRequest(payload: ArrayBuffer, options: RunPickledOptions): ExecPickledRequest {
    const request: ExecPickledRequest = {
      id: this.nextMessageId++,
      kind: 'execPickled',
      payload,
      packages: [...new Set([...this.packages, ...(options.packages ?? [])])],
      wheels: [...new Set(options.wheels ?? [])],
    }
    if (this.pyodideSource !== undefined) request.pyodide = this.pyodideSource
    return request
  }

  private pingRequest(boot: boolean): PingRequest {
    const request: PingRequest = { id: this.nextMessageId++, kind: 'ping' }
    if (boot) request.boot = true
    if (this.pyodideSource !== undefined) request.pyodide = this.pyodideSource
    return request
  }

  /**
   * Wrap one exchange as a worker-pool task that never rejects for
   * protocol-level failures: errors ride back inside the resolved
   * `WorkerResponse` and are unwrapped only after the pool has recycled the
   * worker (a rejected task would permanently shrink the pool).
   */
  private protocolTask<T>(request: ExecRequest): WorkerPoolTask<WorkerResponse<T>> {
    return async (worker) => {
      const { worker: target, response } = await this.exchange<WorkerResponse<T>>(worker, request)
      return { worker: target, result: response }
    }
  }

  /** {@link protocolTask}'s never-reject contract, for execPickled requests. */
  private pickledTask(request: ExecPickledRequest): WorkerPoolTask<ExecPickledResponse> {
    return async (worker) => {
      const { worker: target, response } = await this.exchange<ExecPickledResponse>(
        worker,
        request,
        [request.payload],
      )
      return { worker: target, result: response }
    }
  }

  private async createWorker(): Promise<Worker> {
    const Ctor = await resolveWorkerCtor()
    return new Ctor(this.workerUrl, { type: 'module' })
  }

  /**
   * One request/one response exchange, matched on the message `id` (the
   * worker never posts unsolicited messages). `R` is the response union for
   * the request kind; worker 'error' events (e.g. a bundle that fails to
   * load) resolve as `WorkerFailure` responses regardless of kind, and the
   * worker is remembered as dead and replaced on its next use instead of
   * hanging. `transfer` buffers are moved (not cloned) with the request.
   */
  private async exchange<R extends { id: number; ok: boolean }>(
    worker: Worker | null,
    request: WorkerRequest,
    transfer?: Transferable[],
  ): Promise<{ worker: Worker; response: R | WorkerFailure }> {
    let usable = worker
    if (usable !== null && this.deadWorkers.has(usable)) {
      usable.terminate()
      usable = null
    }
    const target = usable ?? (await this.createWorker())
    const response = await new Promise<R | WorkerFailure>((resolve) => {
      const settle = (value: R | WorkerFailure): void => {
        target.removeEventListener('message', onMessage)
        target.removeEventListener('error', onError)
        resolve(value)
      }
      const onMessage = (event: MessageEvent): void => {
        const data = event.data as R | null
        if (typeof data === 'object' && data !== null && data.id === request.id) {
          settle(data)
        }
      }
      const onError = (event: ErrorEvent): void => {
        this.deadWorkers.add(target)
        settle({
          id: request.id,
          ok: false,
          error: { message: `Worker error before response: ${describeErrorEvent(event)}` },
        })
      }
      target.addEventListener('message', onMessage)
      target.addEventListener('error', onError)
      if (transfer === undefined) {
        target.postMessage(request)
      } else {
        target.postMessage(request, transfer)
      }
    })
    return { worker: target, response }
  }
}
