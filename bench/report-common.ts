/**
 * Report-building blocks shared by the Node (bench/report.ts) and browser
 * (bench/report-browser.ts) markdown generators. Pure string builders —
 * every number is derived from a BenchResults JSON, so regenerating a
 * report from the same file is idempotent. Filesystem access stays in the
 * CLIs (bench/run-bench.ts and bench/report-browser-cli.ts).
 */
import type { BenchCell, BenchResults, WorkloadResult } from './schema.js'
import { fmtMs } from './schema.js'

export function serialCell(workload: WorkloadResult): BenchCell | undefined {
  return workload.cells.find((cell) => cell.poolSize === 1)
}

export function speedupOf(workload: WorkloadResult, cell: BenchCell): number {
  const serial = serialCell(workload)
  return serial === undefined ? Number.NaN : serial.medianMs / cell.medianMs
}

export function workloadTable(workload: WorkloadResult): string[] {
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
export function scalingSentence(workload: WorkloadResult): string {
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

/** YAML front matter of a generated benchmark report (`type: report`). */
export function frontMatter(title: string, created: string, tags: string[], related: string[]): string[] {
  return [
    '---',
    'type: report',
    `title: ${title}`,
    `created: ${created}`,
    'tags:',
    ...tags.map((tag) => `  - ${tag}`),
    'related:',
    ...related.map((name) => `  - '[[${name}]]'`),
    '---',
  ]
}

/** Smoke-mode and verification-failure callouts (empty for a clean run). */
export function runWarnings(results: BenchResults): string[] {
  const lines: string[] = []
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
  return lines
}

/** The "## Workloads" section: preamble plus one titled table per workload. */
export function workloadSections(results: BenchResults): string[] {
  const lines: string[] = [
    '## Workloads',
    '',
    'Every cell runs the same fixed total work; pools are warmed (interpreters',
    'booted, packages installed/mirrored) before timing, and each cell runs one',
    'additional untimed warmup repetition. Efficiency = speedup ÷ workers.',
    '',
  ]
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
  return lines
}

/** One scaling bullet per workload — the head of the Analysis section. */
export function analysisBullets(results: BenchResults): string[] {
  return results.workloads.map((workload) => `- ${scalingSentence(workload)}`)
}
