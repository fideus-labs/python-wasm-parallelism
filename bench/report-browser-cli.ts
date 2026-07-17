/**
 * CLI for the browser benchmark report (`npm run bench:report:browser`):
 * reads the latest bench/results/browser-<date>.json (produced by the @bench
 * Playwright spec, e2e/bench.spec.ts) plus the latest node-<date>.json for
 * the Node-vs-browser comparison section, and writes
 * docs/benchmarks/browser-benchmarks.md. All markdown comes from the pure
 * generator in bench/report-browser.ts; this file owns the filesystem, the
 * same split run-bench.ts has with report.ts.
 */
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import { generateBrowserReport } from './report-browser.js'
import type { BenchResults } from './schema.js'

// tests/helpers.ts exports the same constant, but importing it would pull in
// the pyodide/esbuild fixture stack just to render markdown.
const rootDir = fileURLToPath(new URL('..', import.meta.url))
const resultsDir = path.join(rootDir, 'bench', 'results')

function latestResultsFile(prefix: string): string | null {
  const pattern = new RegExp(`^${prefix}-\\d{4}-\\d{2}-\\d{2}\\.json$`)
  const files = fs.existsSync(resultsDir)
    ? fs
        .readdirSync(resultsDir)
        .filter((name) => pattern.test(name))
        .sort()
    : []
  return files.at(-1) ?? null
}

function readResults(name: string): BenchResults {
  return JSON.parse(fs.readFileSync(path.join(resultsDir, name), 'utf8')) as BenchResults
}

function main(): void {
  const browserName = latestResultsFile('browser')
  if (browserName === null) {
    console.error(
      'No bench/results/browser-<date>.json found — run `npx playwright test --grep @bench` first.',
    )
    process.exitCode = 1
    return
  }
  const nodeName = latestResultsFile('node')
  if (nodeName === null) {
    console.warn('No bench/results/node-<date>.json found — omitting the Node-vs-browser section.')
  }
  const report = generateBrowserReport(
    readResults(browserName),
    `bench/results/${browserName}`,
    nodeName === null ? null : readResults(nodeName),
    nodeName === null ? null : `bench/results/${nodeName}`,
  )
  const reportPath = path.join(rootDir, 'docs', 'benchmarks', 'browser-benchmarks.md')
  fs.mkdirSync(path.dirname(reportPath), { recursive: true })
  fs.writeFileSync(reportPath, report)
  console.log(`report → ${path.relative(rootDir, reportPath)}`)
}

main()
