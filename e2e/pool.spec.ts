/**
 * Browser e2e: the pool demo page (web/) driven through `window.__demo`.
 *
 * Assertions are structural — result equality, completed progress, rendered
 * rows — never absolute durations (speedup numbers belong to the benchmark
 * report; see e2e/bench.spec.ts).
 */
import { expect, openDemo, test } from './fixtures.js'

test('the page is served cross-origin isolated', async ({ page }) => {
  await page.goto('/')
  const isolation = page.getByTestId('isolation')
  await expect(isolation).toHaveText('crossOriginIsolated: true')
  await expect(isolation).toHaveClass(/\bok\b/)
  expect(await page.evaluate(() => window.crossOriginIsolated)).toBe(true)
})

test('pool warmup completes and enables the controls', async ({ page }) => {
  await openDemo(page)
  expect(await page.evaluate(() => window.__demo.isolated)).toBe(true)
  for (const control of ['run-serial', 'run-parallel', 'run-dask']) {
    await expect(page.getByTestId(control)).toBeEnabled()
  }
  // The 4-worker pool actually booted: warmup stats exist for it.
  const setup = await page.evaluate(() => window.__demo.setup())
  const poolSize = await page.evaluate(() => window.__demo.config.poolSize)
  expect(setup.map((stat) => stat.poolSize)).toContain(poolSize)
})

test('a parallel run returns counts identical to the serial run and completes progress', async ({
  page,
}) => {
  await openDemo(page)

  const serial = await page.evaluate(() => window.__demo.runSerial())
  expect(serial.matchesExpected).toBe(true)
  expect(serial.total).toBe(serial.expectedTotal)

  const parallel = await page.evaluate(() => window.__demo.runParallel())
  expect(parallel.counts).toEqual(serial.counts)
  expect(parallel.total).toBe(serial.total)
  expect(parallel.matchesExpected).toBe(true)
  expect(parallel.matchesSerial).toBe(true)

  // The progress bar reached 100%: one tick per chunk, value === max.
  const { chunkCount } = await page.evaluate(() => window.__demo.config)
  expect(parallel.counts).toHaveLength(chunkCount)
  await expect(page.getByTestId('progress-text')).toHaveText(`${chunkCount}/${chunkCount}`)
  // <progress> is driven via its value/max properties (not attributes), so
  // read the live DOM state instead of asserting on attributes.
  expect(
    await page.evaluate(() => {
      const bar = document.querySelector<HTMLProgressElement>('[data-testid="progress"]')
      return bar !== null && bar.max > 0 && bar.value === bar.max
    }),
  ).toBe(true)

  // Both runs rendered a row with the equality check green.
  const equalCells = page.getByTestId('results-body').locator('td[data-cell="equal"]')
  await expect(equalCells).toHaveText(['✓', '✓'])
})

test('a Python exception surfaces as a readable UI error, not an unhandled rejection', async ({
  page,
  consoleErrors,
}) => {
  await openDemo(page)

  const message = await page.evaluate(() => window.__demo.runFailing())
  expect(message).toContain('ValueError: intentional demo failure')

  const errorArea = page.getByRole('alert')
  await expect(errorArea).toBeVisible()
  await expect(errorArea).toContainText('ValueError: intentional demo failure')
  await expect(errorArea).toContainText('Traceback (most recent call last)')

  // The failure was fully handled: the app is ready for the next run and
  // nothing escaped as an uncaught error or unhandled rejection (the auto
  // fixture re-checks this at teardown; this pins it to the failing run).
  await expect(page.getByTestId('status')).toHaveText('ready')
  await expect(page.getByTestId('run-serial')).toBeEnabled()
  expect(consoleErrors).toEqual([])
})
