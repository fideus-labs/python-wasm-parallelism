/**
 * Node demo: embarrassingly parallel prime counting on a PyodidePool.
 *
 * Counts primes in [RANGE_START, RANGE_END) split into CHUNK_COUNT equal
 * ranges, twice: serially on a warmed 1-worker pool, then in parallel on a
 * warmed POOL_SIZE-worker pool via map(). Prints boot times, wall-clocks,
 * per-chunk counts, and the speedup; exits nonzero if the serial and
 * parallel counts disagree.
 *
 * Run with `npm run demo:node`. When invoking tsx directly, run
 * `npm run build` first — workers load the built dist/pyodide-worker.js
 * bundle, not the TypeScript source.
 */
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { PyodidePool } from '../src/index.js'
import type { WarmupResult } from '../src/index.js'

const POOL_SIZE = 4
const CHUNK_COUNT = 8
const RANGE_START = 2
// Sized so one chunk is roughly 1-3 s of pure-Python trial division under
// Pyodide. π(4_000_000) = 283146 — update EXPECTED_TOTAL (or set it to
// null) when changing the range.
const RANGE_END = 4_000_000
const EXPECTED_TOTAL: number | null = 283_146

const rootDir = fileURLToPath(new URL('..', import.meta.url))
const workerFile = path.join(rootDir, 'dist', 'pyodide-worker.js')

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

/**
 * Pure-Python trial division (no numpy) — deliberately CPU-bound so the
 * chunks demonstrate parallelism across interpreters. The final expression
 * is the chunk's prime count, returned by runPythonAsync.
 */
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

function fmt(ms: number): string {
  return ms >= 1000 ? `${(ms / 1000).toFixed(2)} s` : `${Math.round(ms)} ms`
}

function describeBoots(results: WarmupResult[]): string {
  return results.map((r) => `${Math.round(r.bootMs)} ms`).join(', ')
}

async function main(): Promise<void> {
  if (!fs.existsSync(workerFile)) {
    console.error(`Missing ${workerFile} — run \`npm run build\` first (or use \`npm run demo:node\`).`)
    process.exitCode = 1
    return
  }
  const workerUrl = pathToFileURL(workerFile)
  const chunks = makeChunks(RANGE_START, RANGE_END, CHUNK_COUNT)

  console.log(
    `Pyodide worker-pool demo — primes in [${RANGE_START}, ${RANGE_END}) across ${chunks.length} chunks\n`,
  )

  console.log(`Serial baseline: 1 worker, ${chunks.length} chunks sequentially`)
  const serialPool = new PyodidePool({ poolSize: 1, workerUrl })
  const serialWarmStart = performance.now()
  const serialWarm = await serialPool.warmup()
  console.log(
    `  interpreter boot: ${fmt(performance.now() - serialWarmStart)} (worker bootMs: ${describeBoots(serialWarm)})`,
  )
  const serialStart = performance.now()
  const serialCounts: number[] = []
  for (const [index, chunk] of chunks.entries()) {
    const chunkStart = performance.now()
    const count = await serialPool.runPython<number>(primeCountSource(chunk.lo, chunk.hi))
    serialCounts.push(count)
    console.log(
      `  chunk ${index + 1}/${chunks.length} [${chunk.lo}, ${chunk.hi}): ${count} primes in ${fmt(performance.now() - chunkStart)}`,
    )
  }
  const serialMs = performance.now() - serialStart
  serialPool.terminate()
  console.log(`  serial wall-clock: ${fmt(serialMs)}\n`)

  console.log(`Parallel: ${POOL_SIZE} workers, ${chunks.length} chunks via map()`)
  const parallelPool = new PyodidePool({ poolSize: POOL_SIZE, workerUrl })
  const parallelWarmStart = performance.now()
  const parallelWarm = await parallelPool.warmup()
  console.log(
    `  pool boot (${POOL_SIZE} interpreters in parallel): ${fmt(performance.now() - parallelWarmStart)} wall (worker bootMs: ${describeBoots(parallelWarm)})`,
  )
  const parallelStart = performance.now()
  const run = parallelPool.map<number, Chunk>(
    (chunk) => primeCountSource(chunk.lo, chunk.hi),
    chunks,
    {
      onProgress: (completed, total) => {
        console.log(`  progress: ${completed}/${total} chunks done`)
      },
    },
  )
  const parallelCounts = await run.promise
  const parallelMs = performance.now() - parallelStart
  parallelPool.terminate()
  console.log(`  parallel wall-clock: ${fmt(parallelMs)}\n`)

  const serialTotal = serialCounts.reduce((a, b) => a + b, 0)
  const parallelTotal = parallelCounts.reduce((a, b) => a + b, 0)
  const speedup = serialMs / parallelMs
  console.log('Report')
  console.log(`  prime counts per chunk (serial):   [${serialCounts.join(', ')}]`)
  console.log(`  prime counts per chunk (parallel): [${parallelCounts.join(', ')}]`)
  console.log(`  total primes: ${serialTotal} (serial) vs ${parallelTotal} (parallel)`)
  console.log(
    `  serial ${fmt(serialMs)} / parallel ${fmt(parallelMs)} → speedup ${speedup.toFixed(2)}x`,
  )

  const mismatched = chunks
    .map((_, index) => index)
    .filter((index) => serialCounts[index] !== parallelCounts[index])
  if (parallelCounts.length !== serialCounts.length || mismatched.length > 0) {
    console.error(
      `\nFAIL: serial and parallel prime counts disagree (chunks: ${mismatched.join(', ') || 'length mismatch'})`,
    )
    process.exitCode = 1
    return
  }
  if (EXPECTED_TOTAL !== null && serialTotal !== EXPECTED_TOTAL) {
    console.error(`\nFAIL: total ${serialTotal} does not match π(${RANGE_END}) = ${EXPECTED_TOTAL}`)
    process.exitCode = 1
    return
  }
  console.log('  counts match ✓')
  if (speedup < 1.5) {
    console.warn(
      `  WARNING: speedup ${speedup.toFixed(2)}x is below the 1.5x target — check available CPU cores`,
    )
  }
}

main().catch((error: unknown) => {
  console.error(error)
  process.exitCode = 1
})
