/**
 * Browser demo: serial vs pool-parallel vs dask-on-pool prime counting.
 *
 * Reuses the environment-aware Phase 01/02 stack unchanged — PyodidePool +
 * src/worker/pyodide-worker.ts (bundled by Vite via `?worker&url`) and the
 * python/pyodide_pool driver package (bundled as `?raw` source strings and
 * written into the driver's FS, exactly like tests/helpers.ts bootDriver).
 * Only this entry wiring is browser-specific: Pyodide loads from the
 * jsDelivr CDN both on the main thread (the dask driver) and inside the
 * workers (via the PyodideSource carried by every pool request).
 *
 * The whole app is exposed as `window.__demo` with promise-returning
 * methods so Playwright can drive runs deterministically instead of
 * polling the DOM.
 */
import type { PyodideAPI } from 'pyodide'
import { fmtMs } from '../bench/schema'
import { PyodidePool, PyodideTaskError } from '../src/index'
import type { PyodideSource } from '../src/index'
import type { DaskRecord, DemoRecord, PoolSetupStat, RunRecord } from './demo-api'
import initPy from '../python/pyodide_pool/__init__.py?raw'
import bridgePy from '../python/pyodide_pool/_bridge.py?raw'
import packagesPy from '../python/pyodide_pool/_packages.py?raw'
import schedulerPy from '../python/pyodide_pool/scheduler.py?raw'
import workerUrl from '../src/worker/pyodide-worker.ts?worker&url'

// ---------------------------------------------------------------------------
// Configuration — the demo-verified Phase 01/02 workload: π(2_000_000).
// ---------------------------------------------------------------------------

const POOL_SIZE = 4
const RANGE_START = 2
const RANGE_END = 2_000_000
const CHUNK_COUNT = 8
const EXPECTED_TOTAL = 148_933
/** sum(range(RANGE_START, RANGE_END)) — the numpy mirroring workload. */
const NUMPY_EXPECTED_TOTAL = ((RANGE_END - 1) * RANGE_END - (RANGE_START - 1) * RANGE_START) / 2

/** Keep in sync with the `pyodide` version in package.json. */
const PYODIDE_VERSION = '314.0.2'
const PYODIDE_CDN_BASE = `https://cdn.jsdelivr.net/pyodide/v${PYODIDE_VERSION}/full/`
const PYODIDE_MODULE_URL = `${PYODIDE_CDN_BASE}pyodide.mjs`
/** Workers import loadPyodide from the CDN — bare `pyodide` doesn't resolve. */
const PYODIDE_SOURCE: PyodideSource = {
  moduleURL: PYODIDE_MODULE_URL,
  indexURL: PYODIDE_CDN_BASE,
}

/** Same trial-division counter as examples/ and bench/ — pure Python. */
const COUNT_PRIMES_PY = `
def count_primes(lo, hi):
    count = 0
    for n in range(lo, hi):
        if n < 2:
            continue
        if n == 2:
            count += 1
            continue
        if n % 2 == 0:
            continue
        d = 3
        is_prime = True
        while d * d <= n:
            if n % d == 0:
                is_prime = False
                break
            d += 2
        if is_prime:
            count += 1
    return count
`

/** Driver-side pyodide_pool package, written into the driver's FS. */
const PYODIDE_POOL_SOURCES: Record<string, string> = {
  '__init__.py': initPy,
  '_bridge.py': bridgePy,
  '_packages.py': packagesPy,
  'scheduler.py': schedulerPy,
}

interface Chunk {
  readonly lo: number
  readonly hi: number
}

function makeChunks(start: number, end: number, count: number): Chunk[] {
  const width = Math.ceil((end - start) / count)
  const chunks: Chunk[] = []
  for (let lo = start; lo < end; lo += width) {
    chunks.push({ lo, hi: Math.min(lo + width, end) })
  }
  return chunks
}

// ---------------------------------------------------------------------------
// Result records — plain JSON-safe objects so page.evaluate can return them.
// Shapes live in demo-api.ts, shared with the Playwright suite (e2e/).
// ---------------------------------------------------------------------------

export type { DaskRecord, DemoApi, DemoRecord, PoolSetupStat, RunRecord } from './demo-api'

// ---------------------------------------------------------------------------
// DOM wiring
// ---------------------------------------------------------------------------

function element<T extends HTMLElement>(id: string): T {
  const found = document.getElementById(id)
  if (found === null) throw new Error(`missing #${id}`)
  return found as T
}

const statusEl = element<HTMLParagraphElement>('status')
const isolationEl = element<HTMLSpanElement>('isolation')
const errorEl = element<HTMLPreElement>('error')
const progressEl = element<HTMLProgressElement>('progress')
const progressTextEl = element<HTMLSpanElement>('progress-text')
const resultsBodyEl = element<HTMLTableSectionElement>('results-body')
const buttons = {
  serial: element<HTMLButtonElement>('run-serial'),
  parallel: element<HTMLButtonElement>('run-parallel'),
  dask: element<HTMLButtonElement>('run-dask'),
}

function setStatus(text: string): void {
  statusEl.textContent = text
}

function showError(message: string): void {
  errorEl.textContent = message
  errorEl.classList.add('visible')
}

function clearError(): void {
  errorEl.textContent = ''
  errorEl.classList.remove('visible')
}

function setProgress(completed: number, total: number): void {
  progressEl.max = Math.max(total, 1)
  progressEl.value = completed
  progressTextEl.textContent = `${completed}/${total}`
}

function describeError(err: unknown): string {
  if (err instanceof PyodideTaskError && err.pythonTraceback !== undefined) {
    return `${err.message}\n\n${err.pythonTraceback.trimEnd()}`
  }
  return err instanceof Error ? err.message : String(err)
}

function fmtSpeedup(speedup: number | null): string {
  return speedup === null ? '—' : `${speedup.toFixed(2)}×`
}

function addRow(record: DemoRecord): void {
  const equal =
    record.kind === 'dask'
      ? record.equal && record.matchesExpected
      : record.matchesExpected && record.matchesSerial !== false
  const row = document.createElement('tr')
  row.dataset.kind = record.kind
  const cells: [string, string][] = [
    ['label', record.label],
    ['workers', String(record.workers)],
    ['ms', fmtMs(record.ms)],
    ['total', String(record.kind === 'dask' ? record.poolTotal : record.total)],
    ['speedup', fmtSpeedup(record.speedup)],
    ['equal', equal ? '✓' : '✗'],
  ]
  for (const [name, text] of cells) {
    const cell = document.createElement('td')
    cell.dataset.cell = name
    cell.textContent = text
    if (name === 'equal') cell.classList.add(equal ? 'ok' : 'bad')
    row.append(cell)
  }
  resultsBodyEl.append(row)
}

// ---------------------------------------------------------------------------
// Pools and the driver Pyodide
// ---------------------------------------------------------------------------

const pools = new Map<number, Promise<PyodidePool>>()

/** Warmup costs per pool size, in boot order — exposed via __demo.setup(). */
const setupStats: PoolSetupStat[] = []

/** Create-and-warm a pool per size, once — later calls reuse warm workers. */
function poolOfSize(size: number): Promise<PyodidePool> {
  let entry = pools.get(size)
  if (entry === undefined) {
    entry = (async () => {
      const pool = new PyodidePool({
        poolSize: size,
        workerUrl,
        pyodideSource: PYODIDE_SOURCE,
      })
      const started = performance.now()
      const warm = await pool.warmup()
      setupStats.push({
        poolSize: size,
        warmupWallMs: performance.now() - started,
        workerBootMs: warm.map((worker) => worker.bootMs),
        pyodideVersion: warm[0]?.status.pyodideVersion ?? null,
      })
      return pool
    })()
    pools.set(size, entry)
  }
  return entry
}

interface PyodideModule {
  loadPyodide(options?: { indexURL?: string }): Promise<PyodideAPI>
}

let driverPromise: Promise<PyodideAPI> | null = null

/** Boot the main-thread ("driver") Pyodide from the CDN, once. */
function bootDriver(): Promise<PyodideAPI> {
  if (driverPromise === null) {
    driverPromise = (async () => {
      const module = (await import(/* @vite-ignore */ PYODIDE_MODULE_URL)) as PyodideModule
      return module.loadPyodide({ indexURL: PYODIDE_CDN_BASE })
    })().catch((err: unknown) => {
      driverPromise = null // allow a retry
      throw err
    })
  }
  return driverPromise
}

let daskDriverPromise: Promise<PyodideAPI> | null = null

/**
 * First dask run pays the setup once: install dask from PyPI via micropip,
 * write the pyodide_pool sources into the driver FS, register the pool as
 * js_pyodide_pool, and occupy every worker once so each mirrors the
 * driver's package snapshot up front (the tests/helpers.ts bootDaskDriver
 * topology, minus Node's readdirSync).
 */
function ensureDaskDriver(): Promise<PyodideAPI> {
  if (daskDriverPromise === null) {
    daskDriverPromise = (async () => {
      const [driver, pool] = await Promise.all([bootDriver(), poolOfSize(POOL_SIZE)])
      setStatus('installing dask from PyPI into the driver…')
      driver.registerJsModule('js_pyodide_pool', { pool })
      await driver.loadPackage(['cloudpickle', 'micropip'], { messageCallback: () => {} })
      driver.FS.mkdirTree('/driver-site/pyodide_pool')
      for (const [name, source] of Object.entries(PYODIDE_POOL_SOURCES)) {
        driver.FS.writeFile(`/driver-site/pyodide_pool/${name}`, source)
      }
      setStatus('mirroring dask to the workers…')
      await driver.runPythonAsync(`
import sys
sys.path.insert(0, '/driver-site')
import micropip
await micropip.install("dask")
import asyncio, dask, pyodide_pool
await asyncio.gather(*(pyodide_pool.submit(lambda i=i: i) for i in range(${POOL_SIZE})))
`)
      return driver
    })().catch((err: unknown) => {
      daskDriverPromise = null // allow a retry
      throw err
    })
  }
  return daskDriverPromise
}

/** Run driver Python ending in a json.dumps(...) expression; parse it. */
async function runDriverJson<T>(driver: PyodideAPI, code: string): Promise<T> {
  const result: unknown = await driver.runPythonAsync(code)
  return JSON.parse(String(result)) as T
}

// ---------------------------------------------------------------------------
// The three runs
// ---------------------------------------------------------------------------

const history: DemoRecord[] = []
let serialBaseline: { ms: number; counts: number[] } | null = null

function finishRun(record: DemoRecord): DemoRecord {
  history.push(record)
  addRow(record)
  return record
}

async function runSerial(): Promise<RunRecord> {
  const pool = await poolOfSize(POOL_SIZE)
  const chunks = makeChunks(RANGE_START, RANGE_END, CHUNK_COUNT)
  setProgress(0, 1)
  const started = performance.now()
  // All chunks sequentially inside ONE task on one (warm) worker — the same
  // total work as the parallel run, minus any parallelism.
  const counts = await pool.runPython<number[]>(
    `${COUNT_PRIMES_PY}\ncounts = [count_primes(lo, hi) for lo, hi in chunks]\ncounts`,
    { globals: { chunks: chunks.map((chunk) => [chunk.lo, chunk.hi]) } },
  )
  const ms = performance.now() - started
  setProgress(1, 1)
  const total = counts.reduce((sum, count) => sum + count, 0)
  serialBaseline = { ms, counts }
  const record: RunRecord = {
    kind: 'serial',
    label: 'serial',
    workers: 1,
    ms,
    counts,
    total,
    expectedTotal: EXPECTED_TOTAL,
    matchesExpected: total === EXPECTED_TOTAL,
    matchesSerial: true,
    speedup: null,
  }
  finishRun(record)
  return record
}

async function runParallel(workers: number = POOL_SIZE): Promise<RunRecord> {
  const pool = await poolOfSize(workers)
  const chunks = makeChunks(RANGE_START, RANGE_END, CHUNK_COUNT)
  setProgress(0, chunks.length)
  const started = performance.now()
  const run = pool.map<number, [number, number]>(
    `${COUNT_PRIMES_PY}\nlo, hi = item\ncount_primes(lo, hi)`,
    chunks.map((chunk) => [chunk.lo, chunk.hi]),
    {
      onProgress: (completed, total) => {
        setProgress(completed, total)
      },
    },
  )
  const counts = await run.promise
  const ms = performance.now() - started
  const total = counts.reduce((sum, count) => sum + count, 0)
  const matchesSerial =
    serialBaseline === null
      ? null
      : counts.length === serialBaseline.counts.length &&
        counts.every((count, i) => count === serialBaseline?.counts[i])
  const record: RunRecord = {
    kind: 'parallel',
    label: `parallel (${workers} workers)`,
    workers,
    ms,
    counts,
    total,
    expectedTotal: EXPECTED_TOTAL,
    matchesExpected: total === EXPECTED_TOTAL,
    matchesSerial,
    speedup: serialBaseline === null ? null : serialBaseline.ms / ms,
  }
  finishRun(record)
  return record
}

async function runDask(): Promise<DaskRecord> {
  const driver = await ensureDaskDriver()
  const chunks = makeChunks(RANGE_START, RANGE_END, CHUNK_COUNT)
  setStatus('computing the dask graph (synchronous, then on the pool)…')
  setProgress(0, 1)
  const outcome = await runDriverJson<{
    sync_total: number
    pool_total: number
    sync_s: number
    pool_s: number
  }>(
    driver,
    `
import json, time, dask, pyodide_pool
${COUNT_PRIMES_PY}
chunks = ${JSON.stringify(chunks.map((chunk) => [chunk.lo, chunk.hi]))}
leaves = [dask.delayed(count_primes)(lo, hi) for lo, hi in chunks]
total = dask.delayed(sum)(leaves)

t0 = time.perf_counter()
sync_total = total.compute(scheduler="synchronous")
sync_s = time.perf_counter() - t0

t0 = time.perf_counter()
pool_total = await pyodide_pool.compute(total)
pool_s = time.perf_counter() - t0

json.dumps({
    "sync_total": sync_total,
    "pool_total": pool_total,
    "sync_s": sync_s,
    "pool_s": pool_s,
})
`,
  )
  setProgress(1, 1)
  const record: DaskRecord = {
    kind: 'dask',
    label: 'dask graph (pool scheduler)',
    workers: POOL_SIZE,
    ms: outcome.pool_s * 1000,
    syncMs: outcome.sync_s * 1000,
    poolTotal: outcome.pool_total,
    syncTotal: outcome.sync_total,
    matchesExpected: outcome.pool_total === EXPECTED_TOTAL,
    equal: outcome.pool_total === outcome.sync_total,
    speedup: outcome.sync_s / outcome.pool_s,
  }
  finishRun(record)
  return record
}

/**
 * Dask graph whose leaves need numpy on the workers: the driver loads numpy
 * from the CDN, and the first pickled task that uses it triggers
 * pyodide_pool's package mirroring (each worker replays the driver's
 * package snapshot from the same CDN — the tests/dask-scheduler.test.ts
 * numpy topology). Integer sums keep result equality exact.
 */
async function runNumpy(): Promise<DaskRecord> {
  const driver = await ensureDaskDriver()
  setStatus('loading numpy into the driver from the CDN…')
  await driver.loadPackage('numpy', { messageCallback: () => {} })
  const chunks = makeChunks(RANGE_START, RANGE_END, CHUNK_COUNT)
  setStatus('computing the numpy graph (synchronous, then on the pool)…')
  setProgress(0, 1)
  const outcome = await runDriverJson<{
    sync_total: number
    pool_total: number
    sync_s: number
    pool_s: number
  }>(
    driver,
    `
import json, time, dask, pyodide_pool

def chunk_sum(lo, hi):
    import numpy as np
    return int(np.arange(lo, hi, dtype=np.int64).sum())

chunks = ${JSON.stringify(chunks.map((chunk) => [chunk.lo, chunk.hi]))}
leaves = [dask.delayed(chunk_sum)(lo, hi) for lo, hi in chunks]
total = dask.delayed(sum)(leaves)

t0 = time.perf_counter()
sync_total = total.compute(scheduler="synchronous")
sync_s = time.perf_counter() - t0

t0 = time.perf_counter()
pool_total = await pyodide_pool.compute(total)
pool_s = time.perf_counter() - t0

json.dumps({
    "sync_total": sync_total,
    "pool_total": pool_total,
    "sync_s": sync_s,
    "pool_s": pool_s,
})
`,
  )
  setProgress(1, 1)
  const record: DaskRecord = {
    kind: 'dask',
    label: 'dask numpy (pool scheduler)',
    workers: POOL_SIZE,
    ms: outcome.pool_s * 1000,
    syncMs: outcome.sync_s * 1000,
    poolTotal: outcome.pool_total,
    syncTotal: outcome.sync_total,
    matchesExpected: outcome.pool_total === NUMPY_EXPECTED_TOTAL,
    equal: outcome.pool_total === outcome.sync_total,
    speedup: outcome.sync_s / outcome.pool_s,
  }
  finishRun(record)
  return record
}

/**
 * Deliberately raise a Python exception on a worker and surface it through
 * the UI error area — never as an unhandled rejection. Resolves with the
 * message shown, so Playwright can assert on both the return value and the
 * rendered error.
 */
async function runFailing(): Promise<string> {
  const pool = await poolOfSize(POOL_SIZE)
  try {
    await pool.runPython('raise ValueError("intentional demo failure")')
    throw new Error('expected the Python task to fail')
  } catch (err) {
    const message = describeError(err)
    showError(message)
    return message
  }
}

// ---------------------------------------------------------------------------
// Run serialization + init
// ---------------------------------------------------------------------------

let readyDone = false
let busy = false

function updateButtons(): void {
  const disabled = !readyDone || busy
  buttons.serial.disabled = disabled
  buttons.parallel.disabled = disabled
  buttons.dask.disabled = disabled
}

/** One run at a time — runs share the progress bar and the warm pools. */
async function guarded<T>(label: string, fn: () => Promise<T>): Promise<T> {
  if (busy) throw new Error(`cannot start "${label}": a run is already in progress`)
  busy = true
  updateButtons()
  clearError()
  setStatus(`running: ${label}…`)
  try {
    const result = await fn()
    setStatus('ready')
    return result
  } catch (err) {
    setStatus('ready')
    showError(describeError(err))
    throw err
  } finally {
    busy = false
    updateButtons()
  }
}

function wireButton(button: HTMLButtonElement, label: string, fn: () => Promise<unknown>): void {
  button.addEventListener('click', () => {
    // guarded() has already surfaced the error in the UI; swallowing here
    // keeps a failing run from becoming an unhandled rejection.
    guarded(label, fn).catch(() => {})
  })
}

async function init(): Promise<string> {
  const isolated = globalThis.crossOriginIsolated === true
  isolationEl.textContent = `crossOriginIsolated: ${String(isolated)}`
  isolationEl.classList.add(isolated ? 'ok' : 'bad')

  setStatus('booting main Pyodide from the CDN…')
  const driverBoot = bootDriver()
  const poolWarmup = poolOfSize(POOL_SIZE)
  await driverBoot
  setStatus(`warming worker pool (${POOL_SIZE} × Pyodide)…`)
  await poolWarmup
  setStatus('ready')
  readyDone = true
  updateButtons()
  return 'ready'
}

wireButton(buttons.serial, 'serial', runSerial)
wireButton(buttons.parallel, `parallel (${POOL_SIZE} workers)`, () => runParallel(POOL_SIZE))
wireButton(buttons.dask, 'dask graph', runDask)

const ready = init()
ready.catch((err: unknown) => {
  setStatus('startup failed')
  showError(describeError(err))
})

// ---------------------------------------------------------------------------
// window.__demo — the deterministic hook Playwright drives
// ---------------------------------------------------------------------------

window.__demo = {
  ready,
  isolated: globalThis.crossOriginIsolated === true,
  config: {
    poolSize: POOL_SIZE,
    rangeStart: RANGE_START,
    rangeEnd: RANGE_END,
    chunkCount: CHUNK_COUNT,
    expectedTotal: EXPECTED_TOTAL,
    pyodideVersion: PYODIDE_VERSION,
  },
  results: () => [...history],
  setup: () => [...setupStats],
  runSerial: () => guarded('serial', runSerial),
  runParallel: (workers?: number) =>
    guarded(`parallel (${workers ?? POOL_SIZE} workers)`, () => runParallel(workers)),
  runDask: () => guarded('dask graph', runDask),
  runNumpy: () => guarded('dask numpy graph', runNumpy),
  runFailing: () => guarded('failing task', runFailing),
}
