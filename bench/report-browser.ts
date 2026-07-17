/**
 * Human report generator: bench/results/browser-<date>.json →
 * docs/benchmarks/browser-benchmarks.md. Pure — reads nothing, writes
 * nothing; bench/report-browser-cli.ts owns the filesystem
 * (`npm run bench:report:browser`). Every number is derived from the
 * results JSONs (the browser run plus, for the comparison section, the
 * latest Node run), so regenerating from the same files is idempotent.
 * The JSON producer is the @bench Playwright spec (e2e/bench.spec.ts);
 * blocks shared with the Node report live in bench/report-common.ts.
 */
import { analysisBullets, frontMatter, runWarnings, workloadSections } from './report-common.js'
import type { BenchResults, OverheadStat } from './schema.js'
import { fmtMs } from './schema.js'

/**
 * Overheads the browser harness cannot measure use the
 * `{ samples: [], medianMs: NaN }` convention (NaN parses back from JSON
 * as null, hence the isFinite check on top of the schema's number type).
 */
function measured(stat: OverheadStat): boolean {
  return stat.samples.length > 0 && Number.isFinite(stat.medianMs)
}

function ratio(numerator: number, denominator: number): string {
  return `${(numerator / denominator).toFixed(2)}×`
}

/**
 * Per-workload Node-vs-browser tables over the pool sizes present in both
 * runs, keyed by workload id. Today only `primes` overlaps; the section
 * grows automatically if the @bench spec adopts more Node workloads.
 */
function comparisonSection(
  browser: BenchResults,
  node: BenchResults,
  nodeRelPath: string,
): string[] {
  const lines: string[] = [
    '## Node vs browser',
    '',
    'Same machine and the same fixed total work per workload; the Node run is',
    `\`${nodeRelPath}\` ([[node-benchmarks]]). Ratios are browser median ÷ Node`,
    'median (1.00× = parity).',
    '',
  ]

  let worstGapPct = 0
  for (const bWorkload of browser.workloads) {
    const nWorkload = node.workloads.find((w) => w.id === bWorkload.id)
    if (nWorkload === undefined) continue
    const shared = bWorkload.cells.flatMap((bCell) => {
      const nCell = nWorkload.cells.find((c) => c.poolSize === bCell.poolSize)
      return nCell === undefined ? [] : [{ bCell, nCell }]
    })
    if (shared.length === 0) continue
    lines.push(
      `### ${bWorkload.title}`,
      '',
      '| Workers | Node median | Browser median | Browser ÷ Node |',
      '| --- | --- | --- | --- |',
    )
    for (const { bCell, nCell } of shared) {
      const label = bCell.poolSize === 1 ? 'serial (1)' : String(bCell.poolSize)
      worstGapPct = Math.max(worstGapPct, Math.abs(bCell.medianMs / nCell.medianMs - 1) * 100)
      lines.push(
        `| ${label} | ${fmtMs(nCell.medianMs)} | ${fmtMs(bCell.medianMs)} | ${ratio(bCell.medianMs, nCell.medianMs)} |`,
      )
    }
    lines.push('')
    const extra = nWorkload.cells.filter(
      (nCell) => !bWorkload.cells.some((bCell) => bCell.poolSize === nCell.poolSize),
    )
    if (extra.length > 0) {
      const cells = extra
        .map((cell) => `${cell.poolSize} workers → ${fmtMs(cell.medianMs)}`)
        .join(', ')
      lines.push(`Node's matrix additionally measures ${cells}; the browser`)
      lines.push('demo caps its pool at the sizes above.', '')
    }
  }

  const bBoot = browser.overheads.workerBoot
  const nBoot = node.overheads.workerBoot
  if (measured(bBoot) && measured(nBoot)) {
    const bootPct = ((bBoot.medianMs / nBoot.medianMs - 1) * 100).toFixed(0)
    lines.push(
      'CDN package-load costs are the structural difference: a browser worker',
      `boots Pyodide from the jsDelivr CDN — runtime JS, \`pyodide.asm.wasm\`,`,
      'and the stdlib bundle over HTTP — where Node reads `node_modules` from',
      `local disk, so the per-worker boot is ${fmtMs(bBoot.medianMs)} vs ${fmtMs(nBoot.medianMs)}`,
      `(+${bootPct}%) and, unlike Node's, depends on network and HTTP-cache state.`,
      'The prime-counting workload mirrors no packages, so the CDN surcharge',
      'appears only in boot here; first-touch package mirroring (the demo’s',
      'dask and numpy graphs) additionally fetches wheels from PyPI/the Pyodide',
      'CDN at first use — the first-task-pays pattern measured in',
      '[[phase-02-results]], with the network fetch added on top.',
      '',
      `Once workers are warm the gap nearly closes: every shared cell above is`,
      `within ${Math.ceil(worstGapPct)}% of Node. The compute is the same single-threaded`,
      'WebAssembly interpreter in both environments; the residual constant is',
      'browser-side scheduling and messaging overhead, not the workload.',
      '',
    )
  }
  return lines
}

export function generateBrowserReport(
  results: BenchResults,
  resultsRelPath: string,
  nodeResults: BenchResults | null,
  nodeRelPath: string | null,
): string {
  const created = results.createdAt.slice(0, 10)
  const { context: ctx, config, overheads, setup } = results
  const boot = overheads.workerBoot
  const unmeasurable = [overheads.noopRoundTrip, overheads.payloadRoundTrip].filter(
    (stat) => !measured(stat),
  )
  const warmups = [...setup]
    .sort((a, b) => b.poolSize - a.poolSize)
    .map((entry) => `${fmtMs(entry.warmupWallMs)} (${entry.poolSize} workers)`)
    .join(' and ')

  const lines: string[] = [
    ...frontMatter(
      'Browser Benchmarks',
      created,
      ['benchmark', 'browser', 'playwright'],
      ['node-benchmarks', 'phase-02-results'],
    ),
    '',
    '# Browser Benchmarks',
    '',
    'Serial-vs-parallel performance of the Pyodide worker pool in a real',
    'browser: the `@bench` Playwright spec (e2e/bench.spec.ts, run via',
    '`npx playwright test --grep @bench`) drives the `web/` demo’s',
    '`window.__demo` hook in headless Chromium against the production Vite',
    `bundle. This file is regenerated from \`${resultsRelPath}\``,
    '(`npm run bench:report:browser`) — edit the spec or the generator, not',
    'this file. The pool under test is the Phase 01–02 build summarized in',
    '[[phase-02-results]]; the Node counterpart of this report is',
    '[[node-benchmarks]].',
    '',
    ...runWarnings(results),
    '## Environment',
    '',
    '| | |',
    '| --- | --- |',
    `| Date | ${created} |`,
    `| Browser | ${ctx.browser ?? 'unknown'} (headless, cross-origin isolated) |`,
    `| Pyodide | ${ctx.pyodideNpm ?? 'unknown'} (jsDelivr CDN pin) / ${ctx.pyodideRuntime ?? 'unknown'} (runtime) |`,
    `| Test runner | Node ${ctx.node} (Playwright; the workloads run in the browser) |`,
    `| OS | ${ctx.platform} ${ctx.release} (${ctx.arch}) |`,
    `| CPU | ${ctx.cpuModel}, ${ctx.cores} logical cores (availableParallelism ${ctx.availableParallelism}) |`,
    `| Pool sizes | ${config.poolSizes.map((s) => (s === 1 ? '1 (serial)' : String(s))).join(', ')} |`,
    `| Repetitions | ${config.repetitions} timed (median reported) after 1 untimed warmup run per cell |`,
    '',
    ...workloadSections(results),
    '## Overheads',
    '',
    '| Overhead | Median | Samples |',
    '| --- | --- | --- |',
    `| Per-worker Pyodide boot (jsDelivr CDN) | ${fmtMs(boot.medianMs)} | ${boot.samples.length} cold boots |`,
    '',
  ]

  if (unmeasurable.length > 0) {
    lines.push(
      'No-op and payload round-trip floors are not measurable through',
      '`window.__demo` (the page exposes workload methods, not a raw pool',
      'handle); the results JSON records them with the harness’s',
      '`{ samples: [], medianMs: NaN }` unmeasured convention. See',
      '[[node-benchmarks]] for those dispatch floors — the browser shares the',
      'same postMessage/structured-clone task path.',
      '',
    )
  }

  lines.push('## Analysis', '', ...analysisBullets(results), '')
  lines.push(
    `Boot amortization: each worker pays a one-time Pyodide boot of ~${fmtMs(boot.medianMs)},`,
    `fetched from the jsDelivr CDN (median across ${boot.samples.length} cold boots — see the`,
    'CDN cost note below). Warmup boots interpreters in parallel, so the',
    `recorded pool warmups cost ${warmups} of wall-clock,`,
    'and the serial baseline runs on an already-warm worker — all timings',
    'above exclude boot, exactly as in the Node harness.',
    '',
  )

  if (nodeResults !== null && nodeRelPath !== null) {
    lines.push(...comparisonSection(results, nodeResults, nodeRelPath))
  }

  return lines.join('\n')
}
