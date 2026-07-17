/**
 * Browser e2e: the dask.delayed scheduler on the pool, driven through
 * `window.__demo`. The first dask run installs dask from PyPI via micropip;
 * the numpy test additionally exercises pyodide_pool's package mirroring —
 * the driver loads numpy from the jsDelivr CDN and every worker replays the
 * install before unpickling the first task that needs it.
 */
import { expect, openDemo, test } from './fixtures.js'

test('the dask graph computes on the pool and matches the synchronous scheduler', async ({
  page,
}) => {
  await openDemo(page)

  const record = await page.evaluate(() => window.__demo.runDask())
  expect(record.equal).toBe(true)
  expect(record.matchesExpected).toBe(true)
  expect(record.poolTotal).toBe(record.syncTotal)

  const row = page.locator('tr[data-kind="dask"]')
  await expect(row).toHaveCount(1)
  await expect(row.locator('td[data-cell="equal"]')).toHaveText('✓')
  await expect(page.getByTestId('status')).toHaveText('ready')
})

test('a numpy task is mirrored to the workers from the CDN', async ({ page }) => {
  await openDemo(page)

  const record = await page.evaluate(() => window.__demo.runNumpy())
  // The pool total can only exist if the workers imported numpy — i.e. the
  // driver-loaded CDN package was mirrored to them before unpickling.
  expect(record.equal).toBe(true)
  expect(record.matchesExpected).toBe(true)
  expect(record.poolTotal).toBe(record.syncTotal)

  await expect(page.locator('tr[data-kind="dask"] td[data-cell="equal"]')).toHaveText('✓')
  await expect(page.getByTestId('status')).toHaveText('ready')
})
