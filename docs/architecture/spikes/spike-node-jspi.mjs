/**
 * Spike 2 — JSPI `pyodide.ffi.run_sync` over the REAL pool, in Node.
 *
 * Run twice:
 *   node --experimental-wasm-jspi spike-node-jspi.mjs   (expect: run_sync works)
 *   node spike-node-jspi.mjs                            (expect: can_run_sync False)
 *
 * Topology = exact tests/helpers.ts bootDriver: driver Pyodide on the Node
 * main thread, PyodidePool on dist/pyodide-worker.js, python/pyodide_pool
 * written into the driver FS. Question: inside driver Python entered via
 * runPythonAsync, does run_sync(submit(...)) suspend the WASM stack so the
 * JS event loop can deliver the worker's response — i.e. a genuinely
 * blocking-looking Pool.map over the async pool?
 */
import { readFileSync, readdirSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { loadPyodide } from 'pyodide'

const rootDir = fileURLToPath(new URL('../../..', import.meta.url))
const { PyodidePool } = await import(pathToFileURL(path.join(rootDir, 'dist', 'index.js')).href)

console.log('WebAssembly.Suspending:', typeof WebAssembly.Suspending)

const pool = new PyodidePool({
  poolSize: 2,
  workerUrl: pathToFileURL(path.join(rootDir, 'dist', 'pyodide-worker.js')),
})

const api = await loadPyodide()
api.registerJsModule('js_pyodide_pool', { pool })
await api.loadPackage(['cloudpickle', 'micropip'], { messageCallback: () => {} })
const packageDir = path.join(rootDir, 'python', 'pyodide_pool')
api.FS.mkdirTree('/driver-site/pyodide_pool')
for (const name of readdirSync(packageDir)) {
  if (!name.endsWith('.py')) continue
  api.FS.writeFile(`/driver-site/pyodide_pool/${name}`, readFileSync(path.join(packageDir, name), 'utf8'))
}

const out = await api.runPythonAsync(`
import sys, json, time
sys.path.insert(0, '/driver-site')
import pyodide_pool
from pyodide.ffi import can_run_sync, run_sync

report = {"can_run_sync": can_run_sync()}
if can_run_sync():
    t0 = time.time()
    # blocking-looking single submit round-trip through a REAL pool worker
    value = run_sync(pyodide_pool.submit(lambda a, b: a * b, 6, 7))
    report["submit_result"] = value
    report["submit_ms"] = round((time.time() - t0) * 1000)
    # a blocking-looking "map": several tasks, gathered, one run_sync
    import asyncio
    t0 = time.time()
    values = run_sync(asyncio.gather(*(pyodide_pool.submit(lambda i=i: i * i) for i in range(8))))
    report["map_result"] = list(values)
    report["map_ms"] = round((time.time() - t0) * 1000)
else:
    try:
        run_sync(__import__("asyncio").sleep(0))
        report["run_sync_without_jspi"] = "unexpectedly worked"
    except Exception as exc:
        report["run_sync_without_jspi"] = f"{type(exc).__name__}: {exc}"
json.dumps(report)
`)
console.log(out)
pool.terminate()
