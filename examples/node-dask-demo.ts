/**
 * Node demo: dask graphs executed in parallel on a PyodidePool via the
 * pyodide_pool async scheduler.
 *
 * Boots a "driver" Pyodide in the Node main thread, installs dask from PyPI
 * via micropip (dask is NOT in the Pyodide distribution — see
 * docs/architecture/dask-scheduler-design.md) plus the local
 * python/pyodide_pool package (written into the driver's FS), and registers
 * a POOL_SIZE-worker PyodidePool as `js_pyodide_pool`. Three demos, each
 * asserting `await pyodide_pool.compute(...)` matches dask's synchronous
 * scheduler:
 *
 *   1. dask.delayed — CHUNK_COUNT CPU-heavy prime-count leaves (the Phase 01
 *      prime counter) feeding a sum reduction; prints both wall-clocks and
 *      the speedup
 *   2. dask.bag — from_sequence(...).map(...).sum()
 *   3. numpy delayed tasks — the driver loads numpy and workers install it
 *      automatically via package mirroring; prints the first batch (workers
 *      auto-install numpy) vs the second batch (already installed)
 *
 * Exits nonzero on any mismatch. Run with `npm run demo:dask`. When invoking
 * tsx directly, run `npm run build` first — workers load the built
 * dist/pyodide-worker.js bundle, not the TypeScript source.
 */
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { loadPyodide } from 'pyodide'
import type { PyodideAPI } from 'pyodide'
import { PyodidePool } from '../src/index.js'
import type { WarmupResult } from '../src/index.js'

const POOL_SIZE = 4
const CHUNK_COUNT = 8
const RANGE_START = 2
// Sized so the synchronous baseline takes a few seconds while one chunk is
// still sub-second on a worker. π(2_000_000) = 148933 — update EXPECTED_TOTAL
// (or set it to null) when changing the range.
const RANGE_END = 2_000_000
const EXPECTED_TOTAL: number | null = 148_933

const rootDir = fileURLToPath(new URL('..', import.meta.url))
const workerFile = path.join(rootDir, 'dist', 'pyodide-worker.js')
const packageDir = path.join(rootDir, 'python', 'pyodide_pool')

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

function fmt(ms: number): string {
  return ms >= 1000 ? `${(ms / 1000).toFixed(2)} s` : `${Math.round(ms)} ms`
}

function fmtS(seconds: number): string {
  return fmt(seconds * 1000)
}

function describeBoots(results: WarmupResult[]): string {
  return results.map((r) => `${Math.round(r.bootMs)} ms`).join(', ')
}

async function main(): Promise<void> {
  if (!fs.existsSync(workerFile)) {
    console.error(`Missing ${workerFile} — run \`npm run build\` first (or use \`npm run demo:dask\`).`)
    process.exitCode = 1
    return
  }
  const workerUrl = pathToFileURL(workerFile)
  const chunks = makeChunks(RANGE_START, RANGE_END, CHUNK_COUNT)

  console.log(
    `Pyodide dask demo — pyodide_pool async scheduler on ${POOL_SIZE} workers vs dask's synchronous scheduler\n`,
  )

  const pool = new PyodidePool({ poolSize: POOL_SIZE, workerUrl })
  let driver: PyodideAPI

  /** Run driver Python ending in a json.dumps(...) expression; parse it. */
  async function run<T>(code: string): Promise<T> {
    const result: unknown = await driver.runPythonAsync(code)
    return JSON.parse(String(result)) as T
  }

  const failures: string[] = []

  try {
    console.log('Setup')
    let t = performance.now()
    const warm = await pool.warmup()
    console.log(
      `  worker pool boot (${POOL_SIZE} interpreters in parallel): ${fmt(performance.now() - t)} (worker bootMs: ${describeBoots(warm)})`,
    )

    t = performance.now()
    driver = await loadPyodide()
    console.log(`  driver Pyodide boot: ${fmt(performance.now() - t)}`)
    driver.registerJsModule('js_pyodide_pool', { pool })

    t = performance.now()
    await driver.loadPackage(['cloudpickle', 'micropip'], { messageCallback: () => {} })
    driver.FS.mkdirTree('/driver-site/pyodide_pool')
    for (const name of fs.readdirSync(packageDir)) {
      if (!name.endsWith('.py')) continue
      driver.FS.writeFile(
        `/driver-site/pyodide_pool/${name}`,
        fs.readFileSync(path.join(packageDir, name), 'utf8'),
      )
    }
    await driver.runPythonAsync(`
import sys
sys.path.insert(0, '/driver-site')
import micropip
# dask is NOT in the Pyodide distribution — install from PyPI.
await micropip.install("dask")
import dask, pyodide_pool
`)
    console.log(
      `  driver packages (cloudpickle, dask from PyPI, pyodide_pool via FS): ${fmt(performance.now() - t)}`,
    )

    t = performance.now()
    await driver.runPythonAsync(`
import asyncio, pyodide_pool
# ${POOL_SIZE} concurrent submits occupy every worker, so each mirrors the
# driver's installed packages (dask and its dependencies) once, up front.
await asyncio.gather(*(pyodide_pool.submit(lambda i=i: i) for i in range(${POOL_SIZE})))
`)
    console.log(
      `  package mirroring warm-up (dask → ${POOL_SIZE} workers): ${fmt(performance.now() - t)}\n`,
    )

    // ---- Demo 1: dask.delayed prime-count graph -------------------------
    console.log(
      `Demo 1: dask.delayed — primes in [${RANGE_START}, ${RANGE_END}) across ${chunks.length} chunks feeding sum()`,
    )
    const d1 = await run<{
      sync_total: number
      pool_total: number
      sync_s: number
      pool_s: number
    }>(`
import json, time, dask, pyodide_pool

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

chunks = ${JSON.stringify(chunks.map((c) => [c.lo, c.hi]))}
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
`)
    const speedup = d1.sync_s / d1.pool_s
    console.log(`  synchronous scheduler: ${fmtS(d1.sync_s)} (total = ${d1.sync_total})`)
    console.log(`  pyodide_pool.compute:  ${fmtS(d1.pool_s)} (total = ${d1.pool_total})`)
    console.log(`  speedup: ${speedup.toFixed(2)}x`)
    if (d1.pool_total !== d1.sync_total) {
      failures.push(`demo 1: pool total ${d1.pool_total} != synchronous total ${d1.sync_total}`)
    } else if (EXPECTED_TOTAL !== null && d1.sync_total !== EXPECTED_TOTAL) {
      failures.push(`demo 1: total ${d1.sync_total} does not match π(${RANGE_END}) = ${EXPECTED_TOTAL}`)
    } else {
      console.log('  results match ✓')
    }
    if (speedup < 1.5) {
      console.warn(
        `  WARNING: speedup ${speedup.toFixed(2)}x is below the 1.5x target — check available CPU cores`,
      )
    }

    // ---- Demo 2: dask.bag map/sum ---------------------------------------
    console.log(`\nDemo 2: dask.bag — from_sequence(range(24)).map(busy_square).sum()`)
    const d2 = await run<{
      sync_val: number
      pool_val: number
      sync_s: number
      pool_s: number
    }>(`
import json, time, dask
import dask.bag as db
import pyodide_pool

def busy_square(x):
    acc = 0
    for _ in range(200_000):
        acc = (acc + x * x) % 1_000_003
    return acc

bag = db.from_sequence(range(24), npartitions=${POOL_SIZE}).map(busy_square).sum()

t0 = time.perf_counter()
sync_val = bag.compute(scheduler="synchronous")
sync_s = time.perf_counter() - t0

t0 = time.perf_counter()
pool_val = await pyodide_pool.compute(bag)
pool_s = time.perf_counter() - t0

json.dumps({
    "sync_val": sync_val,
    "pool_val": pool_val,
    "sync_s": sync_s,
    "pool_s": pool_s,
})
`)
    console.log(`  synchronous scheduler: ${fmtS(d2.sync_s)} (sum = ${d2.sync_val})`)
    console.log(`  pyodide_pool.compute:  ${fmtS(d2.pool_s)} (sum = ${d2.pool_val})`)
    if (d2.pool_val !== d2.sync_val) {
      failures.push(`demo 2: pool sum ${d2.pool_val} != synchronous sum ${d2.sync_val}`)
    } else {
      console.log('  results match ✓')
    }

    // ---- Demo 3: numpy delayed tasks (package mirroring) ----------------
    console.log('\nDemo 3: numpy delayed tasks — workers install numpy via package mirroring')
    t = performance.now()
    await driver.loadPackage('numpy', { messageCallback: () => {} })
    console.log(`  driver numpy load: ${fmt(performance.now() - t)}`)
    const d3 = await run<{
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

n = 1_000_000
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
    console.log(
      `  first batch of ${POOL_SIZE} (workers auto-install numpy): ${fmtS(d3.first_s)}`,
    )
    console.log(`  second batch of ${POOL_SIZE} (numpy already installed): ${fmtS(d3.second_s)}`)
    if (!d3.first_ok || !d3.second_ok) {
      failures.push(
        `demo 3: worker results disagree with driver-local numpy (first_ok=${d3.first_ok}, second_ok=${d3.second_ok})`,
      )
    } else {
      console.log('  results match driver-local numpy ✓')
    }
  } finally {
    pool.terminate()
  }

  console.log('\nReport')
  if (failures.length > 0) {
    for (const failure of failures) console.error(`  FAIL: ${failure}`)
    process.exitCode = 1
    return
  }
  console.log('  all demos passed ✓')
}

main().catch((error: unknown) => {
  console.error(error)
  process.exitCode = 1
})
