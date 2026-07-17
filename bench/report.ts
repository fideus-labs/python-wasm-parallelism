/**
 * Human report generator: bench/results/node-<date>.json →
 * docs/benchmarks/node-benchmarks.md. Pure — reads nothing, writes nothing;
 * bench/run-bench.ts owns the filesystem (both after a bench run and via
 * `npm run bench:report`). Every number in the report is derived from the
 * results JSON, so regenerating from the same file is idempotent. Blocks
 * shared with the browser report (bench/report-browser.ts) live in
 * bench/report-common.ts.
 */
import { analysisBullets, frontMatter, runWarnings, workloadSections } from './report-common.js'
import type { BenchResults } from './schema.js'
import { fmtMs } from './schema.js'

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
    ...frontMatter(
      'Node.js Benchmarks',
      created,
      ['benchmark', 'node', 'pyodide'],
      ['phase-02-results', 'browser-benchmarks'],
    ),
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
    ...runWarnings(results),
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
    ...workloadSections(results),
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
    ...analysisBullets(results),
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
  ]

  return lines.join('\n')
}
