/**
 * Shared test base for the browser suite.
 *
 * Every test gets an auto console watchdog: `console.error` output and
 * uncaught page errors (`pageerror` fires for uncaught exceptions AND
 * unhandled promise rejections) fail the test at teardown unless they match
 * the allow-list of known-benign Pyodide messages. Specs can also read the
 * collected `consoleErrors` mid-test to pin "nothing escaped" to a precise
 * moment (e.g. right after a deliberately failing run).
 */
import { test as base, expect, type Page } from '@playwright/test'
import type { DemoApi } from '../web/demo-api.js'

// Referenced for its `declare global { interface Window { __demo } }`
// augmentation, which types the page.evaluate callbacks in every spec.
export type { DemoApi }

/**
 * Known-benign console noise. Pyodide's package loader prints
 * "Loading <pkgs>" / "Loaded <pkgs>" lines while the workers mirror
 * packages (flagged by the Phase 04 smoke test as the allow-list
 * candidates); they usually arrive as console.log but are allowed at any
 * severity so a Pyodide bump can't flake the suite.
 */
export const ALLOWED_CONSOLE_ERRORS: RegExp[] = [/^Loading [\w, .-]+$/, /^Loaded [\w, .-]+$/]

interface DemoFixtures {
  /** Unexpected console.error / pageerror lines collected so far. */
  consoleErrors: string[]
}

/**
 * Build a test base whose console watchdog allows the given patterns (in
 * addition to nothing else). The JupyterLite spec extends the shared
 * allow-list with JupyterLab-specific noise without loosening this suite.
 */
export function makeConsoleWatchdogTest(allowedPatterns: RegExp[]) {
  return base.extend<DemoFixtures>({
    consoleErrors: [
      async ({ page }, use) => {
        const unexpected: string[] = []
        page.on('console', (message) => {
          if (message.type() !== 'error') return
          const text = message.text()
          if (allowedPatterns.some((pattern) => pattern.test(text))) return
          unexpected.push(`console.error: ${text}`)
        })
        page.on('pageerror', (error) => {
          if (allowedPatterns.some((pattern) => pattern.test(error.message))) return
          unexpected.push(`pageerror: ${error.message}`)
        })
        await use(unexpected)
        expect(unexpected, 'unexpected console errors / uncaught page errors').toEqual([])
      },
      { auto: true },
    ],
  })
}

export const test = makeConsoleWatchdogTest(ALLOWED_CONSOLE_ERRORS)

export { expect } from '@playwright/test'

/**
 * Load the demo and wait until it is fully booted: main-thread Pyodide plus
 * the 4-worker pool, all fetched from the jsDelivr CDN — covered by the
 * config-level 120 s expect budget.
 */
export async function openDemo(page: Page): Promise<void> {
  await page.goto('/')
  await expect(page.getByTestId('status')).toHaveText('ready')
  expect(await page.evaluate(() => window.__demo.ready)).toBe('ready')
}
