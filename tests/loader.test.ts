/**
 * python/pyodide_pool/loader.py against the real browser bundle: a driver
 * Pyodide (Node main thread) imports the self-contained ESM file through
 * `run_js("import('<url>')")` — the exact mechanism JupyterLite notebooks
 * rely on — constructs the JS pool through its createPool export, and
 * installs the Python wrapper as the default pool. Task execution over
 * nested workers is browser-only (blob workers do not exist in
 * worker_threads) and is covered by the e2e suite instead.
 */
import { pathToFileURL } from 'node:url'
import { afterAll, beforeAll, expect, it } from 'vitest'
import { buildBrowserBundle } from '../scripts/bundles.mjs'
import type { PyodidePool } from '../src/index.js'
import { bootDriver, buildWorkerBundle, createPool } from './helpers.js'
import type { PyodideDriver } from './helpers.js'

let bundleUrl: string
let pool: PyodidePool
let driver: PyodideDriver

beforeAll(async () => {
  await buildWorkerBundle()
  const { outfile } = await buildBrowserBundle()
  bundleUrl = pathToFileURL(outfile).href
  // bootDriver needs a pool to register as js_pyodide_pool; the tests below
  // deliberately ignore it — create_pool must build its own from the bundle.
  pool = await createPool(1)
  driver = await bootDriver(pool)
})

afterAll(() => {
  pool.terminate()
})

it('create_pool imports the bundle, sizes the JS pool, and returns the wrapper', async () => {
  const outcome = await driver.run<{
    type: string
    pool_size: number
    is_default: boolean
    distinct_from_registered: boolean
  }>(`
import json
import pyodide_pool
from pyodide_pool.loader import create_pool

registered = pyodide_pool.default_pool()
loaded = await create_pool(pool_size=2, js_url=${JSON.stringify(bundleUrl)})
json.dumps({
    "type": type(loaded).__name__,
    "pool_size": loaded.pool_size,
    "is_default": pyodide_pool.default_pool() is loaded,
    "distinct_from_registered": loaded is not registered,
})
`)
  expect(outcome.type).toBe('WorkerPool')
  expect(outcome.pool_size).toBe(2)
  expect(outcome.is_default).toBe(true)
  expect(outcome.distinct_from_registered).toBe(true)
})

it('create_pool is re-exported lazily from the package root', async () => {
  const outcome = await driver.run<{ same: boolean; in_all: boolean }>(`
import json
import pyodide_pool
from pyodide_pool.loader import create_pool
json.dumps({
    "same": pyodide_pool.create_pool is create_pool,
    "in_all": "create_pool" in pyodide_pool.__all__,
})
`)
  expect(outcome.same).toBe(true)
  expect(outcome.in_all).toBe(true)
})

it('set_default_pool(None) resets to js_pyodide_pool resolution', async () => {
  const outcome = await driver.run<{ registered_again: boolean }>(`
import json
import pyodide_pool

pyodide_pool.set_default_pool(None)
fresh = pyodide_pool.default_pool()
import js_pyodide_pool
# Two JsProxy wrappers of one JS object are never \`is\`-identical; js_id is.
json.dumps({"registered_again": fresh._js_pool.js_id == js_pyodide_pool.pool.js_id})
`)
  expect(outcome.registered_again).toBe(true)
})
