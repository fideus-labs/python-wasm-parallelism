/**
 * Human report generator: bench/results/node-<date>.json →
 * docs/benchmarks/node-benchmarks.md. Pure — reads nothing, writes nothing;
 * bench/run-bench.ts owns the filesystem (both after a bench run and via
 * `npm run bench:report`). Every number in the report is derived from the
 * results JSON, so regenerating from the same file is idempotent.
 */
import type { BenchCell, BenchResults, WorkloadResult } from './schema.js'
import { fmtMs } from './schema.js'

function serialCell(workload: WorkloadResult): BenchCell | undefined {
  return workload.cells.find((cell) => cell.poolSize === 1)
}

function speedupOf(workload: WorkloadResult, cell: BenchCell): number {
  const serial = serialCell(workload)
  return serial === undefined ? Number.NaN : serial.medianMs / cell.medianMs
}

function workloadTable(workload: WorkloadResult): string[] {
  const rows = workload.cells.map((cell) => {
    const label = cell.poolSize === 1 ? 'serial (1)' : String(cell.poolSize)
    const speedup = speedupOf(workload, cell)
    const efficiency = (speedup / cell.poolSize) * 100
    return `| ${label} | ${fmtMs(cell.medianMs)} | ${speedup.toFixed(2)}× | ${efficiency.toFixed(0)}% |`
  })
  return [
    '| Workers | Median wall-clock | Speedup | Efficiency |',
    '| --- | --- | --- | --- |',
    ...rows,
  ]
}

/**
 * One data-driven scaling sentence per workload: headline speedup at the
 * largest pool, plus where the marginal gain from doubling workers tapers
 * (marginal < 1.4× for a 2× worker increase).
 */
function scalingSentence(workload: WorkloadResult): string {
  const cells = [...workload.cells].sort((a, b) => a.poolSize - b.poolSize)
  const largest = cells.at(-1)
  if (largest === undefined || largest.poolSize === 1) {
    return `**${workload.title}** — no parallel cells recorded.`
  }
  const headline = speedupOf(workload, largest)
  const efficiency = (headline / largest.poolSize) * 100
  let taper: string | null = null
  for (let i = 0; i + 1 < cells.length; i++) {
    const a = cells[i]
    const b = cells[i + 1]
    if (a === undefined || b === undefined || b.poolSize !== a.poolSize * 2) continue
    const marginal = speedupOf(workload, b) / speedupOf(workload, a)
    if (marginal < 1.4 && taper === null) {
      const unit = a.poolSize === 1 ? 'worker' : 'workers'
      taper = ` Gains taper past ${a.poolSize} ${unit} (${a.poolSize}→${b.poolSize} only added ${marginal.toFixed(2)}× where ideal is 2×).`
    }
  }
  return (
    `**${workload.title}** reaches ${headline.toFixed(2)}× on ${largest.poolSize} workers ` +
    `(${efficiency.toFixed(0)}% efficiency).${taper ?? ' Scaling holds through the largest pool measured.'}`
  )
}

export function generateReport(results: BenchResults, resultsRelPath: string): string {
  const created = results.createdAt.slice(0, 10)
  const { context: ctx, config, overheads } = results
  const maxSize = Math.max(...config.poolSizes)
  const boot = overheads.workerBoot
  const noop = overheads.noopRoundTrip
  const payload = overheads.payloadRoundTrip
  const payloadMiB = payload.payloadBytes / (1024 * 1024)
  const perMiB = (payload.medianMs - noop.medianMs) / payloadMiB

  const lines: string[] = [
    '---',
    'type: report',
    'title: Node.js Benchmarks',
    `created: ${created}`,
    'tags:',
    '  - benchmark',
    '  - node',
    '  - pyodide',
    'related:',
    "  - '[[phase-02-results]]'",
    "  - '[[browser-benchmarks]]'",
    '---',
    '',
    '# Node.js Benchmarks',
    '',
    'Serial-vs-parallel performance of the Pyodide worker pool in Node,',
    'measured by `npm run bench` (bench/run-bench.ts). This file is',
    `regenerated from \`${resultsRelPath}\` (\`npm run bench:report\`) — edit`,
    'the harness or the generator, not this file. The pool and async dask',
    'scheduler under test are the Phase 01–02 builds summarized in',
    '[[phase-02-results]]; the browser counterpart of this report is',
    '[[browser-benchmarks]] (Phase 04).',
    '',
  ]

  if (results.mode === 'smoke') {
    lines.push(
      '> [!WARNING]',
      '> Smoke-mode run: tiny workloads and a reduced matrix, used only to',
      '> validate the harness. Numbers are not representative.',
      '',
    )
  }
  if (results.failures.length > 0) {
    lines.push('> [!CAUTION]', '> This run recorded verification failures:')
    for (const failure of results.failures) lines.push(`> - ${failure}`)
    lines.push('')
  }

  lines.push(
    '## Environment',
    '',
    '| | |',
    '| --- | --- |',
    `| Date | ${created} |`,
    `| Node | ${ctx.node} |`,
    `| Pyodide | ${ctx.pyodideNpm ?? 'unknown'} (npm) / ${ctx.pyodideRuntime ?? 'unknown'} (runtime) |`,
    `| OS | ${ctx.platform} ${ctx.release} (${ctx.arch}) |`,
    `| CPU | ${ctx.cpuModel}, ${ctx.cores} logical cores (availableParallelism ${ctx.availableParallelism}) |`,
    `| Pool sizes | ${config.poolSizes.map((s) => (s === 1 ? '1 (serial)' : String(s))).join(', ')} |`,
    `| Repetitions | ${config.repetitions} timed (median reported) after 1 untimed warmup run per cell |`,
    '',
    '## Workloads',
    '',
    'Every cell runs the same fixed total work; pools are warmed (interpreters',
    'booted, packages installed/mirrored) before timing, and each cell runs one',
    'additional untimed warmup repetition. Efficiency = speedup ÷ workers.',
    '',
  )

  for (const workload of results.workloads) {
    lines.push(
      `### ${workload.title}`,
      '',
      `${workload.description} Fixed total work: ${workload.totalWork}.`,
      '',
      ...workloadTable(workload),
      '',
    )
  }

  lines.push(
    '## Overheads',
    '',
    '| Overhead | Median | Samples |',
    '| --- | --- | --- |',
    `| Per-worker Pyodide boot | ${fmtMs(boot.medianMs)} | ${boot.samples.length} cold boots |`,
    `| No-op task round-trip (warm worker) | ${fmtMs(noop.medianMs)} | ${noop.samples.length} |`,
    `| ${payloadMiB.toFixed(0)} MiB numpy cloudpickle round-trip | ${fmtMs(payload.medianMs)} | ${payload.samples.length} |`,
    '',
    'Per-cell warmup durations (first-touch package installs and mirroring',
    'replays) are recorded in the results JSON under `cells[].warmupMs` and',
    '`setup[]`.',
    '',
    '## Analysis',
    '',
  )

  for (const workload of results.workloads) {
    lines.push(`- ${scalingSentence(workload)}`)
  }

  lines.push(
    '',
    `Boot amortization: each worker pays a one-time Pyodide boot of ~${fmtMs(boot.medianMs)}`,
    `(median across ${boot.samples.length} cold boots). Warmup boots interpreters in parallel, so a`,
    `pool is ready in roughly one boot's wall-clock, and recycled workers keep`,
    'their interpreter (`bootMs` = 0 on reuse). All timings above exclude boot;',
    'a one-shot script whose total Python work is comparable to a single boot',
    'cannot amortize the pool.',
    '',
    `Serialization and dispatch: a no-op task round-trips in ~${fmtMs(noop.medianMs)}, and a`,
    `${payloadMiB.toFixed(0)} MiB float64 cloudpickle payload echo takes ~${fmtMs(payload.medianMs)}`,
    `(≈ ${fmtMs(perMiB)} per MiB over the no-op floor, covering pickle + structured-clone`,
    'transfer + unpickle in both directions). Tasks should stay coarse relative',
    'to these floors — the dask.bag finding from [[phase-02-results]] (tasks of',
    '~90 ms barely profit) is the same effect.',
    '',
    `Core count: the machine exposes ${ctx.cores} logical cores (availableParallelism`,
    `${ctx.availableParallelism}); the matrix caps at ${maxSize} workers, each a single-threaded`,
    'WebAssembly interpreter, so ideal speedup equals the worker count as long',
    'as the chunk count divides evenly across workers — the taper points above',
    'mark where per-task overhead and shared-machine effects win instead.',
    '',
  )

  return lines.join('\n')
}
