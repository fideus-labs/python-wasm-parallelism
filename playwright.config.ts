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
 *
 * Two projects, two servers: `chromium` runs the Vite demo suite against
 * `vite preview` (4173); `jupyterlite` runs the JupyterLite smoke test
 * against `serve-lite.mjs` (8000, same COOP/COEP headers). webServer
 * entries are config-global, so both servers start regardless of
 * `--project` filtering — build:lite is doit-cached and near-instant when
 * nothing changed.
 */
import { defineConfig, devices } from '@playwright/test'

const PREVIEW_URL = 'http://localhost:4173'
const LITE_URL = 'http://localhost:8000'

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
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
      testIgnore: /jupyterlite\.spec\.ts/,
    },
    {
      name: 'jupyterlite',
      use: { ...devices['Desktop Chrome'], baseURL: LITE_URL },
      testMatch: /jupyterlite\.spec\.ts/,
    },
  ],
  webServer: [
    {
      // --strictPort: if 4173 is taken by something that isn't a live preview
      // server, fail loudly instead of silently testing the wrong port.
      command: 'npm run build:web && npm run preview -- --strictPort',
      url: PREVIEW_URL,
      reuseExistingServer: !process.env.CI,
      timeout: 180_000,
      stdout: 'pipe',
      stderr: 'pipe',
    },
    {
      // The health-check URL points at a real built file so a stale/empty
      // _output (or a foreign process squatting on 8000) fails the check
      // instead of green-lighting a 404-serving server.
      command: 'npm run build:lite && npm run serve:lite',
      url: `${LITE_URL}/lab/index.html`,
      reuseExistingServer: !process.env.CI,
      timeout: 300_000,
      stdout: 'pipe',
      stderr: 'pipe',
    },
  ],
})
