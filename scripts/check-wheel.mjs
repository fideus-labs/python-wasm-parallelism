// Quick Node verification of the JupyterLite payload — `npm run check:wheel`:
//
//   1. dist/pyodide-pool.browser.js imports as a bare ES module (no bundler,
//      no import map — the way JupyterLite's kernel worker will load it) and
//      exposes the expected API.
//   2. The built wheel (demos/jupyterlite/files/wheels/) installs via
//      micropip into a bare Pyodide and pyodide_pool imports cleanly from
//      site-packages, matching the wheel's version.
//
// Prerequisites: `npm run build` (or build:lite) for the bundle, and
// `node scripts/build-lite-assets.mjs` (or build:lite) for the wheel.
import { readFileSync, readdirSync } from 'node:fs'
import path from 'node:path'
import { pathToFileURL } from 'node:url'
import { loadPyodide } from 'pyodide'
import { rootDir } from './bundles.mjs'

let failures = 0
function check(label, ok, detail = '') {
  console.log(`${ok ? '✓' : '✗'} ${label}${detail === '' ? '' : ` — ${detail}`}`)
  if (!ok) failures += 1
}

// --- 1. the self-contained browser bundle -----------------------------------
const bundlePath = path.join(rootDir, 'dist', 'pyodide-pool.browser.js')
const bundle = await import(pathToFileURL(bundlePath).href)
check('bundle imports as a bare ES module', true, path.relative(rootDir, bundlePath))
check('bundle exports createPool', typeof bundle.createPool === 'function')
check('bundle exports PyodidePool', typeof bundle.PyodidePool === 'function')
const pool = bundle.createPool({ poolSize: 2 })
check('createPool({poolSize: 2}) constructs without spawning workers', pool.poolSize === 2)
check(
  'bundled CDN default matches installed pyodide',
  bundle.PYODIDE_VERSION === (await import('pyodide/package.json', { with: { type: 'json' } })).default.version,
  `v${bundle.PYODIDE_VERSION}`,
)

// --- 2. the wheel, installed via micropip into a bare Pyodide ----------------
const wheelDir = path.join(rootDir, 'demos', 'jupyterlite', 'files', 'wheels')
const wheels = readdirSync(wheelDir)
  .filter((name) => name.startsWith('pyodide_pool-') && name.endsWith('.whl'))
  .sort()
if (wheels.length === 0) {
  console.error(`✗ no pyodide_pool wheel in ${wheelDir} — run: node scripts/build-lite-assets.mjs`)
  process.exit(1)
}
const wheelName = wheels[wheels.length - 1]
const wheelVersion = wheelName.split('-')[1]

const py = await loadPyodide()
await py.loadPackage('micropip', { messageCallback: () => {} })
py.FS.writeFile(`/tmp/${wheelName}`, readFileSync(path.join(wheelDir, wheelName)))
const summary = JSON.parse(
  await py.runPythonAsync(`
import json, micropip
await micropip.install("emfs:/tmp/${wheelName}")

import pyodide_pool
from pyodide_pool import WorkerPool, compute, get, snapshot_packages, submit
from pyodide_pool.loader import DEFAULT_JS_URL, create_pool

json.dumps({
    "version": pyodide_pool.__version__,
    "installed_from": micropip.list()["pyodide-pool"].source,
    "create_pool_is_coroutine": __import__("inspect").iscoroutinefunction(create_pool),
    "default_js_url": DEFAULT_JS_URL,
})
`),
)
check('micropip installs the wheel', summary.installed_from.endsWith('.whl'), wheelName)
check('pyodide_pool imports from site-packages', summary.version === wheelVersion, `v${summary.version}`)
check('loader.create_pool is an async function', summary.create_pool_is_coroutine === true)
check('DEFAULT_JS_URL points at the site asset', summary.default_js_url === '/files/assets/pyodide-pool.browser.js')

process.exit(failures === 0 ? 0 : 1)
