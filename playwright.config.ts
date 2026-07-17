/**
 * Playwright config for the browser e2e suite (e2e/).
 *
 * The suite always exercises the production bundle: the webServer entry
 * rebuilds web/ and serves it with `vite preview`, which sends the COOP/COEP
 * headers required for cross-origin isolation (see vite.config.ts) — the dev
 * server is never under test.
 *
 * Timeout budgets are sized for Pyodide: every page boots a main interpreter
 * plus a multi-worker pool, all fetched from the jsDelivr CDN on first use,
 * so readiness assertions legitimately wait tens of seconds. Interaction
 * timeouts stay tight so genuine UI failures still fail fast.
 *
 * Benchmark specs are tagged @bench and excluded from the default run by the
 * `test:browser` npm script (`--grep-invert @bench`); run them explicitly
 * with `npx playwright test --grep @bench`. The exclusion lives in the
 * script, not here — a config-level grepInvert would combine with a CLI
 * `--grep @bench` and match nothing.
 */
import { defineConfig, devices } from '@playwright/test'

const PREVIEW_URL = 'http://localhost:4173'

export default defineConfig({
  testDir: 'e2e',
  // Every test page boots its own Pyodide interpreters (main + pool workers,
  // ~150 MB of WASM each). Parallel spec files would contend for CPU, slow
  // warmups past their budgets, and skew the timings the @bench spec
  // records — so files run one at a time.
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  reporter: [['list'], ['html', { open: 'never' }]],
  timeout: 240_000,
  expect: { timeout: 120_000 },
  use: {
    baseURL: PREVIEW_URL,
    trace: 'on-first-retry',
    screenshot: 'only-on-failure',
    actionTimeout: 15_000,
    navigationTimeout: 30_000,
  },
  projects: [{ name: 'chromium', use: { ...devices['Desktop Chrome'] } }],
  webServer: {
    // --strictPort: if 4173 is taken by something that isn't a live preview
    // server, fail loudly instead of silently testing the wrong port.
    command: 'npm run build:web && npm run preview -- --strictPort',
    url: PREVIEW_URL,
    reuseExistingServer: !process.env.CI,
    timeout: 180_000,
    stdout: 'pipe',
    stderr: 'pipe',
  },
})
