/**
 * Browser benchmark (@bench) — excluded from `npm run test:browser`
 * (`--grep-invert @bench`); run explicitly with
 * `npx playwright test --grep @bench`.
 *
 * Mirrors the Node harness's results schema (bench/schema.ts): the serial
 * and the 2-/4-worker parallel prime workloads via `window.__demo`, one
 * untimed warmup run then REPETITIONS timed runs per cell, written to
 * bench/results/browser-<iso-date>.json. Correctness is asserted
 * structurally (totals and per-chunk counts); durations are recorded for
 * the report, never asserted.
 */
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import type { BenchCell, BenchResults, PoolSetup } from '../bench/schema.js'
import { fmtMs, median } from '../bench/schema.js'
import type { RunRecord } from '../web/demo-api.js'
import { expect, openDemo, test } from './fixtures.js'

const REPETITIONS = 3
/** Parallel pool sizes; 1 (the serial baseline) is always recorded first. */
const PARALLEL_SIZES = [2, 4]

test(
  'primes serial vs 2/4-worker pool → bench/results/browser-<date>.json',
  { tag: '@bench' },
  async ({ page, browser, browserName }) => {
    // 12 timed Pyodide workload runs plus CDN boots — a wide, single budget.
    test.setTimeout(600_000)

    await openDemo(page)
    const config = await page.evaluate(() => window.__demo.config)
    const failures: string[] = []

    const checkRun = (record: RunRecord, label: string): void => {
      if (!record.matchesExpected) {
        failures.push(`${label}: total ${record.total} !== expected ${record.expectedTotal}`)
      }
      if (record.matchesSerial === false) {
        failures.push(`${label}: per-chunk counts differ from the serial baseline`)
      }
    }

    const cells: BenchCell[] = []

    // Serial baseline: every chunk sequentially in ONE task on one warm
    // worker — recorded as poolSize 1, the Node report's "serial (1)" row.
    const serialWarmup = await page.evaluate(() => window.__demo.runSerial())
    checkRun(serialWarmup, 'serial warmup')
    const serialRuns: RunRecord[] = []
    for (let i = 0; i < REPETITIONS; i++) {
      const run = await page.evaluate(() => window.__demo.runSerial())
      checkRun(run, `serial run ${i + 1}`)
      serialRuns.push(run)
    }
    const serialMs = serialRuns.map((run) => run.ms)
    cells.push({
      poolSize: 1,
      warmupMs: serialWarmup.ms,
      runsMs: serialMs,
      medianMs: median(serialMs),
      value: serialRuns[0]?.total ?? Number.NaN,
    })
    console.log(`serial: median ${fmtMs(median(serialMs))}`)

    for (const workers of PARALLEL_SIZES) {
      // The first run per size is the untimed warmup; it also absorbs the
      // pool boot for sizes the page has not warmed yet (2 — size 4 booted
      // during init).
      const warmup = await page.evaluate((size) => window.__demo.runParallel(size), workers)
      checkRun(warmup, `parallel(${workers}) warmup`)
      const runs: RunRecord[] = []
      for (let i = 0; i < REPETITIONS; i++) {
        const run = await page.evaluate((size) => window.__demo.runParallel(size), workers)
        checkRun(run, `parallel(${workers}) run ${i + 1}`)
        runs.push(run)
      }
      const runsMs = runs.map((run) => run.ms)
      cells.push({
        poolSize: workers,
        warmupMs: warmup.ms,
        runsMs,
        medianMs: median(runsMs),
        value: runs[0]?.total ?? Number.NaN,
      })
      console.log(`parallel(${workers}): median ${fmtMs(median(runsMs))}`)
    }

    const values = new Set(cells.map((cell) => cell.value))
    if (values.size !== 1) {
      failures.push(`verification values disagree across cells: ${[...values].join(', ')}`)
    }

    // Pool warmup costs recorded by the page (size 4 at init, 2 on first
    // use). The primes workload mirrors no packages, hence mirrorWarmMs 0.
    const setupStats = await page.evaluate(() => window.__demo.setup())
    const setup: PoolSetup[] = setupStats.map((stat) => ({
      poolSize: stat.poolSize,
      warmupWallMs: stat.warmupWallMs,
      workerBootMs: stat.workerBootMs,
      mirrorWarmMs: 0,
    }))
    const bootSamples = setup.flatMap((entry) => entry.workerBootMs).filter((ms) => ms > 0)

    const userAgent = await page.evaluate(() => navigator.userAgent)
    const cpus = os.cpus()

    const results: BenchResults = {
      schema: 1,
      createdAt: new Date().toISOString(),
      mode: 'full',
      context: {
        // The runner's Node — the workload itself runs in `browser`.
        node: process.version,
        pyodideNpm: config.pyodideVersion,
        pyodideRuntime:
          setupStats.find((stat) => stat.pyodideVersion !== null)?.pyodideVersion ?? null,
        platform: os.platform(),
        release: os.release(),
        arch: os.arch(),
        cpuModel: cpus[0]?.model ?? 'unknown',
        cores: cpus.length,
        availableParallelism: os.availableParallelism(),
        browser: `${browserName} ${browser.version()}`,
        userAgent,
      },
      config: {
        poolSizes: [1, ...PARALLEL_SIZES],
        repetitions: REPETITIONS,
        primes: {
          rangeStart: config.rangeStart,
          rangeEnd: config.rangeEnd,
          chunkCount: config.chunkCount,
          expectedTotal: config.expectedTotal,
        },
      },
      setup,
      workloads: [
        {
          id: 'primes',
          title: 'Prime counting (pure Python, CPU-bound)',
          description:
            'Trial-division prime counting from the browser demo, driven through `window.__demo` in headless Chromium (serial = every chunk sequentially in one task on a warm worker; parallel = `pool.map()` across the chunks).',
          totalWork: `primes in [${config.rangeStart}, ${config.rangeEnd}) across ${config.chunkCount} chunks`,
          cells,
        },
      ],
      overheads: {
        workerBoot: {
          samples: bootSamples,
          medianMs: bootSamples.length > 0 ? median(bootSamples) : Number.NaN,
        },
        // Not measurable through window.__demo (no raw pool handle) — the
        // same `{ samples: [], medianMs: NaN }` convention run-bench.ts
        // uses for unmeasured overheads (NaN serializes to null).
        noopRoundTrip: { samples: [], medianMs: Number.NaN },
        payloadRoundTrip: { samples: [], medianMs: Number.NaN, payloadBytes: 0 },
      },
      failures,
    }

    const configFile = test.info().config.configFile
    const rootDir = configFile === undefined ? process.cwd() : path.dirname(configFile)
    const resultsDir = path.join(rootDir, 'bench', 'results')
    fs.mkdirSync(resultsDir, { recursive: true })
    const jsonPath = path.join(resultsDir, `browser-${results.createdAt.slice(0, 10)}.json`)
    fs.writeFileSync(jsonPath, `${JSON.stringify(results, null, 2)}\n`)
    console.log(`wrote ${path.relative(rootDir, jsonPath)}`)

    expect(failures).toEqual([])
  },
)
