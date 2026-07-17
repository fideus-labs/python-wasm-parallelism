/**
 * Benchmark harness: serial-vs-parallel Pyodide-pool performance across
 * worker counts and workload types (Phase 03).
 *
 * Four workloads over the same fixed total work per cell:
 *   a. prime counting   — pure-Python trial division (Phase 01), pool.map()
 *   b. Monte Carlo π    — random-heavy pure Python, pool.map()
 *   c. numpy matmul     — driver-side pyodide_pool.submit() batch: package
 *                         mirroring installs numpy on workers and the full
 *                         float64 product matrices round-trip via cloudpickle
 *   d. dask.delayed     — prime-count leaves feeding sum(), executed by the
 *                         Phase 02 async scheduler (pyodide_pool.compute)
 *
 * Matrix: warmed size-1 pool (serial baseline) vs pool sizes 2/4/8 capped at
 * os.availableParallelism(); per cell 1 untimed warmup run + `repetitions`
 * timed runs (performance.now()), median reported. Overheads measured
 * separately: per-worker Pyodide boot, no-op task round-trip, and a 1 MiB
 * numpy cloudpickle echo. All boot/driver plumbing is reused from
 * tests/helpers.ts — the same fixtures the test suites drive.
 *
 * Usage:
 *   npm run bench                # full matrix → bench/results/node-<date>.json
 *                                #   + docs/benchmarks/node-benchmarks.md
 *   npm run bench -- --smoke     # tiny harness self-test → bench/results/node-smoke*
 *   npm run bench:report         # regenerate the report from the latest JSON
 */
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import type { PyodidePool } from '../src/index.js'
import { bootDriver, createPool, rootDir } from '../tests/helpers.js'
import type { PyodideDriver } from '../tests/helpers.js'
import { generateReport } from './report.js'
import type {
  BenchCell,
  NodeBenchConfig,
  BenchResults,
  OverheadStat,
  PoolSetup,
  WorkloadResult,
} from './schema.js'
import { fmtMs, median } from './schema.js'

const now = (): number => performance.now()

const resultsDir = path.join(rootDir, 'bench', 'results')

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

function fullConfig(): NodeBenchConfig {
  const cap = os.availableParallelism()
  return {
    poolSizes: [1, 2, 4, 8].filter((size) => size <= cap),
    repetitions: 3,
    // π(2_000_000) = 148933 — the demo-verified Phase 01/02 workload size.
    primes: { rangeStart: 2, rangeEnd: 2_000_000, chunkCount: 8, expectedTotal: 148_933 },
    // ~5M samples/s/worker measured → ~3 s serial for the 16M-sample total.
    pi: { chunkCount: 8, samplesPerChunk: 2_000_000 },
    matmul: { taskCount: 8, n: 256, k: 20 },
    dask: { rangeStart: 2, rangeEnd: 2_000_000, chunkCount: 8, expectedTotal: 148_933 },
    noopReps: 10,
    payloadReps: 5,
    payloadFloats: 131_072, // 1 MiB of float64
  }
}

/** Tiny matrix that exercises every code path in under ~2 minutes. */
function smokeConfig(): NodeBenchConfig {
  const cap = os.availableParallelism()
  return {
    poolSizes: [1, 2].filter((size) => size <= cap),
    repetitions: 1,
    primes: { rangeStart: 2, rangeEnd: 50_000, chunkCount: 4, expectedTotal: null },
    pi: { chunkCount: 4, samplesPerChunk: 50_000 },
    matmul: { taskCount: 4, n: 64, k: 4 },
    dask: { rangeStart: 2, rangeEnd: 50_000, chunkCount: 4, expectedTotal: null },
    noopReps: 5,
    payloadReps: 2,
    payloadFloats: 131_072,
  }
}

// ---------------------------------------------------------------------------
// Workload Python sources
// ---------------------------------------------------------------------------

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

/** Pure-Python trial division — the Phase 01 demo workload, verbatim. */
function primeCountSource(lo: number, hi: number): string {
  return `
def _count_primes(lo, hi):
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

_count_primes(${lo}, ${hi})
`
}

/** Seeded in-circle hit counting; deterministic per (seed, samples). */
function monteCarloSource(seed: number, samples: number): string {
  return `
import random

def _mc_hits(seed, samples):
    rng = random.Random(seed)
    hits = 0
    for _ in range(samples):
        x = rng.random()
        y = rng.random()
        if x * x + y * y <= 1.0:
            hits += 1
    return hits

_mc_hits(${seed}, ${samples})
`
}

/**
 * Driver-side definitions, run once per bench: install dask from PyPI
 * (not in the Pyodide distribution), define the matmul task (numpy pickled
 * by reference — workers get it via package mirroring) and the delayed
 * prime-count reduction graph. The graph is pool-independent; each pool is
 * attached later via `_wp`.
 */
function driverSetupSource(daskChunks: Chunk[]): string {
  const chunkPairs = JSON.stringify(daskChunks.map((chunk) => [chunk.lo, chunk.hi]))
  return `
import asyncio, json, time
import micropip
await micropip.install("dask")
import dask
import numpy as np

def bench_count_primes(lo, hi):
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

def bench_matmul(seed, n, k):
    rng = np.random.default_rng(seed)
    a = rng.standard_normal((n, n))
    b = rng.standard_normal((n, n))
    c = a @ b
    for _ in range(k - 1):
        c = (c / np.abs(c).max()) @ b
    return c

_bench_leaves = [dask.delayed(bench_count_primes)(lo, hi) for lo, hi in ${chunkPairs}]
_bench_total = dask.delayed(sum)(_bench_leaves)
`
}

/**
 * Wrap the current pool (set as `js_bench_pool` on the driver globals) and
 * occupy each worker once so every interpreter mirrors the driver's package
 * snapshot (dask, numpy, ...) before any timed run.
 */
function attachPoolSource(poolSize: number): string {
  return `
import asyncio, pyodide_pool
_wp = pyodide_pool.WorkerPool(js_bench_pool)
await asyncio.gather(*(_wp.submit(lambda i=i: i) for i in range(${poolSize})))
`
}

// ---------------------------------------------------------------------------
// Measurement helpers
// ---------------------------------------------------------------------------

function sum(values: readonly number[]): number {
  return values.reduce((a, b) => a + b, 0)
}

function valuesClose(a: number, b: number): boolean {
  return Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(a), Math.abs(b))
}

interface CellContext {
  reps: number
  failures: string[]
}

/** One untimed warmup run, then `reps` timed runs; the median is the cell. */
async function runCell(
  id: string,
  poolSize: number,
  ctx: CellContext,
  rep: () => Promise<number>,
): Promise<BenchCell> {
  const warmStart = now()
  const value = await rep()
  const warmupMs = now() - warmStart
  const runsMs: number[] = []
  for (let i = 0; i < ctx.reps; i++) {
    const start = now()
    const repValue = await rep()
    const ms = now() - start
    runsMs.push(ms)
    if (!valuesClose(repValue, value)) {
      ctx.failures.push(
        `${id} pool=${poolSize} rep ${i + 1}: value ${repValue} != warmup value ${value}`,
      )
    }
  }
  console.log(
    `  [${id}] pool=${poolSize}: median ${fmtMs(median(runsMs))} ` +
      `(runs ${runsMs.map((ms) => fmtMs(ms)).join(', ')}; warmup ${fmtMs(warmupMs)})`,
  )
  return { poolSize, warmupMs, runsMs, medianMs: median(runsMs), value }
}

/** Round-trip latency of a no-op exec on a warmed pool. */
async function measureNoop(pool: PyodidePool, reps: number): Promise<OverheadStat> {
  await pool.runPython('None') // settle first-touch costs outside the samples
  const samples: number[] = []
  for (let i = 0; i < reps; i++) {
    const start = now()
    await pool.runPython('None')
    samples.push(now() - start)
  }
  return { samples, medianMs: median(samples) }
}

/** Echo a 1 MiB float64 array through pyodide_pool.submit (cloudpickle both ways). */
async function measurePayload(
  driver: PyodideDriver,
  config: NodeBenchConfig,
  failures: string[],
): Promise<OverheadStat & { payloadBytes: number }> {
  await driver.api.runPythonAsync(
    `_bench_payload = np.arange(${config.payloadFloats}, dtype=np.float64)`,
  )
  const echo = '_bench_echo = await _wp.submit(lambda a: a, _bench_payload)'
  await driver.api.runPythonAsync(echo) // warmup (mirrors numpy if needed)
  const samples: number[] = []
  for (let i = 0; i < config.payloadReps; i++) {
    const start = now()
    await driver.api.runPythonAsync(echo)
    samples.push(now() - start)
  }
  const intact = await driver.run<boolean>(
    'json.dumps(bool((_bench_echo == _bench_payload).all()))',
  )
  if (!intact) {
    failures.push('payload round-trip: echoed 1 MiB array differs from the original')
  }
  return { samples, medianMs: median(samples), payloadBytes: config.payloadFloats * 8 }
}

/**
 * os.cpus() reports model "unknown" on some ARM Linux machines (e.g. WSL2),
 * where /proc/cpuinfo carries no `model name` either — only the ARM
 * implementer/part ID registers, so fall back to those.
 */
function cpuModel(): string {
  const fromOs = os.cpus()[0]?.model
  if (fromOs !== undefined && fromOs !== '' && fromOs !== 'unknown') return fromOs
  try {
    const info = fs.readFileSync('/proc/cpuinfo', 'utf8')
    const modelName = /^model name\s*:\s*(.+)$/m.exec(info)?.[1]
    if (modelName !== undefined) return modelName.trim()
    const implementer = /^CPU implementer\s*:\s*(\S+)$/m.exec(info)?.[1]
    const part = /^CPU part\s*:\s*(\S+)$/m.exec(info)?.[1]
    if (implementer !== undefined && part !== undefined) {
      const vendors: Record<string, string> = { '0x41': 'ARM', '0x51': 'Qualcomm', '0x61': 'Apple' }
      const vendor = vendors[implementer] ?? `implementer ${implementer}`
      return `${vendor} ARM64 (part ${part})`
    }
  } catch {
    // /proc/cpuinfo unreadable (non-Linux) — fall through to "unknown"
  }
  return fromOs ?? 'unknown'
}

function pyodideNpmVersion(): string | null {
  try {
    const raw = fs.readFileSync(path.join(rootDir, 'node_modules', 'pyodide', 'package.json'), 'utf8')
    const pkg = JSON.parse(raw) as { version?: string }
    return pkg.version ?? null
  } catch {
    return null
  }
}

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

async function runBench(mode: 'full' | 'smoke'): Promise<void> {
  const config = mode === 'smoke' ? smokeConfig() : fullConfig()
  const failures: string[] = []
  const startedAt = new Date()
  const benchStart = now()

  const primesChunks = makeChunks(
    config.primes.rangeStart,
    config.primes.rangeEnd,
    config.primes.chunkCount,
  )
  const daskChunks = makeChunks(config.dask.rangeStart, config.dask.rangeEnd, config.dask.chunkCount)
  const piSeeds = Array.from({ length: config.pi.chunkCount }, (_, i) => i)
  const piTotalSamples = config.pi.chunkCount * config.pi.samplesPerChunk

  const workloads = {
    primes: {
      id: 'primes',
      title: 'Prime counting (pure Python, CPU-bound)',
      description:
        'Trial-division prime counting from Phase 01; equal ranges dispatched with `pool.map()`.',
      totalWork: `primes in [${config.primes.rangeStart}, ${config.primes.rangeEnd}) across ${config.primes.chunkCount} chunks`,
      cells: [],
    } as WorkloadResult,
    pi: {
      id: 'montecarlo-pi',
      title: 'Monte Carlo π estimation (random-heavy pure Python)',
      description:
        'Seeded `random.Random` sampling; each chunk counts in-circle hits, dispatched with `pool.map()`.',
      totalWork: `${piTotalSamples.toLocaleString('en-US')} samples across ${config.pi.chunkCount} chunks`,
      cells: [],
    } as WorkloadResult,
    matmul: {
      id: 'numpy-matmul',
      title: 'numpy batch matmul (mirrored packages + serialization)',
      description:
        'Driver Python submits cloudpickled numpy tasks via `pyodide_pool.submit`; workers mirror ' +
        'numpy, chain matmuls, and return the full float64 product matrix through cloudpickle.',
      totalWork: `${config.matmul.taskCount} tasks × ${config.matmul.k} chained matmuls of ${config.matmul.n}×${config.matmul.n}`,
      cells: [],
    } as WorkloadResult,
    dask: {
      id: 'dask-delayed',
      title: 'dask.delayed reduction graph (Phase 02 scheduler)',
      description:
        '`dask.delayed` prime-count leaves feeding `sum()`, executed by the async scheduler via ' +
        '`pyodide_pool.compute(..., pool=...)`.',
      totalWork: `primes in [${config.dask.rangeStart}, ${config.dask.rangeEnd}) across ${config.dask.chunkCount} delayed leaves`,
      cells: [],
    } as WorkloadResult,
  }

  const setup: PoolSetup[] = []
  const bootSamples: number[] = []
  let pyodideRuntime: string | null = null
  let driver: PyodideDriver | null = null
  let noop: OverheadStat | null = null
  let payload: (OverheadStat & { payloadBytes: number }) | null = null

  console.log(
    `Node benchmark harness (${mode}) — pool sizes [${config.poolSizes.join(', ')}], ` +
      `${config.repetitions} timed reps per cell\n`,
  )

  for (const poolSize of config.poolSizes) {
    console.log(`── pool size ${poolSize} ──────────────────────────────`)
    const pool = await createPool(poolSize)
    try {
      const warmStart = now()
      const warm = await pool.warmup()
      const warmupWallMs = now() - warmStart
      const workerBootMs = warm.map((result) => result.bootMs)
      bootSamples.push(...workerBootMs)
      pyodideRuntime ??= warm[0]?.status.pyodideVersion ?? null
      console.log(
        `  pool warmup: ${fmtMs(warmupWallMs)} (worker bootMs: ${workerBootMs
          .map((ms) => Math.round(ms))
          .join(', ')})`,
      )

      if (poolSize === 1) {
        noop = await measureNoop(pool, config.noopReps)
        console.log(`  no-op round-trip median: ${fmtMs(noop.medianMs)}`)
      }

      const ctx: CellContext = { reps: config.repetitions, failures }

      workloads.primes.cells.push(
        await runCell('primes', poolSize, ctx, async () => {
          const counts = await pool.map<number, Chunk>(
            (chunk) => primeCountSource(chunk.lo, chunk.hi),
            primesChunks,
          ).promise
          return sum(counts)
        }),
      )

      workloads.pi.cells.push(
        await runCell('montecarlo-pi', poolSize, ctx, async () => {
          const hits = await pool.map<number, number>(
            (seed) => monteCarloSource(seed, config.pi.samplesPerChunk),
            piSeeds,
          ).promise
          return sum(hits)
        }),
      )

      if (driver === null) {
        console.log('  booting driver Pyodide (cloudpickle + micropip, dask from PyPI, numpy)...')
        driver = await bootDriver(pool)
        await driver.api.loadPackage('numpy', { messageCallback: () => {} })
        await driver.api.runPythonAsync(driverSetupSource(daskChunks))
      }
      const mirrorStart = now()
      driver.api.globals.set('js_bench_pool', pool)
      await driver.api.runPythonAsync(attachPoolSource(poolSize))
      const mirrorWarmMs = now() - mirrorStart
      console.log(`  package mirroring warm-up: ${fmtMs(mirrorWarmMs)}`)
      setup.push({ poolSize, warmupWallMs, workerBootMs, mirrorWarmMs })
      const boundDriver = driver

      workloads.matmul.cells.push(
        await runCell('numpy-matmul', poolSize, ctx, () =>
          boundDriver.run<number>(`
_res = await asyncio.gather(*(_wp.submit(bench_matmul, i, ${config.matmul.n}, ${config.matmul.k}) for i in range(${config.matmul.taskCount})))
json.dumps(float(sum(float(np.trace(r)) for r in _res)))
`),
        ),
      )

      workloads.dask.cells.push(
        await runCell('dask-delayed', poolSize, ctx, () =>
          boundDriver.run<number>(`
_total = await pyodide_pool.compute(_bench_total, pool=_wp)
json.dumps(_total)
`),
        ),
      )

      if (poolSize === 1) {
        payload = await measurePayload(boundDriver, config, failures)
        console.log(`  1 MiB cloudpickle round-trip median: ${fmtMs(payload.medianMs)}`)
      }
    } finally {
      pool.terminate()
    }
  }

  // -- Cross-size verification ----------------------------------------------
  for (const workload of Object.values(workloads)) {
    const baseline = workload.cells[0]
    if (baseline === undefined) continue
    for (const cell of workload.cells) {
      if (!valuesClose(cell.value, baseline.value)) {
        failures.push(
          `${workload.id}: pool=${cell.poolSize} value ${cell.value} != serial value ${baseline.value}`,
        )
      }
    }
  }
  const primesBaseline = workloads.primes.cells[0]
  if (
    config.primes.expectedTotal !== null &&
    primesBaseline !== undefined &&
    primesBaseline.value !== config.primes.expectedTotal
  ) {
    failures.push(
      `primes: total ${primesBaseline.value} != π(${config.primes.rangeEnd}) = ${config.primes.expectedTotal}`,
    )
  }
  const daskBaseline = workloads.dask.cells[0]
  if (
    config.dask.expectedTotal !== null &&
    daskBaseline !== undefined &&
    daskBaseline.value !== config.dask.expectedTotal
  ) {
    failures.push(
      `dask-delayed: total ${daskBaseline.value} != π(${config.dask.rangeEnd}) = ${config.dask.expectedTotal}`,
    )
  }
  const piBaseline = workloads.pi.cells[0]
  if (piBaseline !== undefined) {
    const estimate = (4 * piBaseline.value) / piTotalSamples
    if (Math.abs(estimate - Math.PI) > 0.01) {
      failures.push(`montecarlo-pi: estimate ${estimate} deviates from π by more than 0.01`)
    }
  }
  if (noop === null || payload === null) {
    failures.push('overheads were not measured (no size-1 pool in the matrix?)')
  }

  const results: BenchResults = {
    schema: 1,
    createdAt: startedAt.toISOString(),
    mode,
    context: {
      node: process.version,
      pyodideNpm: pyodideNpmVersion(),
      pyodideRuntime,
      platform: os.platform(),
      release: os.release(),
      arch: os.arch(),
      cpuModel: cpuModel(),
      cores: os.cpus().length,
      availableParallelism: os.availableParallelism(),
    },
    config,
    setup,
    workloads: Object.values(workloads),
    overheads: {
      workerBoot: { samples: bootSamples, medianMs: median(bootSamples) },
      noopRoundTrip: noop ?? { samples: [], medianMs: Number.NaN },
      payloadRoundTrip: payload ?? { samples: [], medianMs: Number.NaN, payloadBytes: 0 },
    },
    failures,
  }

  fs.mkdirSync(resultsDir, { recursive: true })
  const jsonName = mode === 'smoke' ? 'node-smoke.json' : `node-${results.createdAt.slice(0, 10)}.json`
  const jsonPath = path.join(resultsDir, jsonName)
  fs.writeFileSync(jsonPath, `${JSON.stringify(results, null, 2)}\n`)
  console.log(`\nresults → ${path.relative(rootDir, jsonPath)}`)

  if (failures.length === 0) {
    writeReport(results, `bench/results/${jsonName}`)
  } else {
    console.error('\nFAIL: verification failures — report not regenerated:')
    for (const failure of failures) console.error(`  - ${failure}`)
    process.exitCode = 1
  }
  console.log(`total bench time: ${fmtMs(now() - benchStart)}`)
}

// ---------------------------------------------------------------------------
// Report regeneration
// ---------------------------------------------------------------------------

function writeReport(results: BenchResults, resultsRelPath: string): void {
  const reportPath =
    results.mode === 'smoke'
      ? path.join(resultsDir, 'node-smoke-report.md')
      : path.join(rootDir, 'docs', 'benchmarks', 'node-benchmarks.md')
  fs.mkdirSync(path.dirname(reportPath), { recursive: true })
  fs.writeFileSync(reportPath, generateReport(results, resultsRelPath))
  console.log(`report → ${path.relative(rootDir, reportPath)}`)
}

function regenerateFromLatest(): void {
  const files = fs.existsSync(resultsDir)
    ? fs
        .readdirSync(resultsDir)
        .filter((name) => /^node-\d{4}-\d{2}-\d{2}\.json$/.test(name))
        .sort()
    : []
  const latest = files.at(-1)
  if (latest === undefined) {
    console.error('No bench/results/node-<date>.json found — run `npm run bench` first.')
    process.exitCode = 1
    return
  }
  const results = JSON.parse(fs.readFileSync(path.join(resultsDir, latest), 'utf8')) as BenchResults
  writeReport(results, `bench/results/${latest}`)
}

async function main(): Promise<void> {
  const args = process.argv.slice(2)
  if (args.includes('--report-only')) {
    regenerateFromLatest()
    return
  }
  await runBench(args.includes('--smoke') ? 'smoke' : 'full')
}

main().catch((error: unknown) => {
  console.error(error)
  process.exitCode = 1
})
