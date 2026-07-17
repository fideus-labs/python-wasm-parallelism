/**
 * Spike 3 — browser, JupyterLite-kernel-shaped topology (Playwright chromium).
 *
 * JupyterLite runs the Pyodide kernel in a Web Worker; our pool workers would
 * be NESTED workers spawned from it. Serve a COOP/COEP page and measure:
 *  A. main thread: Atomics.wait legality                     (expect: throws)
 *  B. worker: crossOriginIsolated + Atomics.wait legality    (expect: ok)
 *  C. worker blocked in Atomics.wait, NESTED worker writes SAB + notify
 *     → does the blocked kernel-shaped worker wake?          (expect: yes)
 *  D. worker blocked, nested worker postMessage → delivered? (expect: no —
 *     same event-loop starvation as Node; the existing pool protocol cannot
 *     complete under a blocked kernel)
 *  E. typeof WebAssembly.Suspending in page + worker (JSPI availability in
 *     current Chromium, incl. workers)
 */
import http from 'node:http'
import { chromium } from '@playwright/test'

const NESTED = `
onmessage = (e) => {
  const ia = new Int32Array(e.data.sab)
  if (e.data.mode === 'sab') {
    setTimeout(() => { Atomics.store(ia, 1, 77); Atomics.store(ia, 0, 1); Atomics.notify(ia, 0) }, 100)
  } else {
    setTimeout(() => postMessage('nested-result'), 100)
  }
}
`

const KERNEL = `
const nested = new Worker('/nested.js')
const report = {}
report.crossOriginIsolated = self.crossOriginIsolated
report.suspendingInWorker = typeof WebAssembly.Suspending
// B: bare legality in a worker
try {
  const ia = new Int32Array(new SharedArrayBuffer(4))
  report.waitInWorker = Atomics.wait(ia, 0, 0, 30)
} catch (e) { report.waitInWorker = 'threw: ' + e.constructor.name }
// C: nested worker writes SAB + notify while this (kernel) worker blocks
{
  const sab = new SharedArrayBuffer(8)
  const ia = new Int32Array(sab)
  nested.postMessage({ mode: 'sab', sab })
  const t0 = Date.now()
  report.sabWake = { outcome: Atomics.wait(ia, 0, 0, 5000), elapsedMs: Date.now() - t0, value: Atomics.load(ia, 1) }
}
// D: nested worker postMessage while this worker blocks
{
  let delivered = null
  nested.onmessage = (e) => { delivered = e.data }
  nested.postMessage({ mode: 'post', sab: new SharedArrayBuffer(4) })
  const gate = new Int32Array(new SharedArrayBuffer(4))
  Atomics.wait(gate, 0, 0, 1000)
  report.postDeliveredWhileBlocked = delivered
  setTimeout(() => { report.postDeliveredAfterUnblock = delivered; postMessage(report) }, 100)
}
`

const PAGE = `<!doctype html><script>
const report = { pageCrossOriginIsolated: self.crossOriginIsolated, suspendingInPage: typeof WebAssembly.Suspending }
try {
  const ia = new Int32Array(new SharedArrayBuffer(4))
  report.waitOnMain = Atomics.wait(ia, 0, 0, 10)
} catch (e) { report.waitOnMain = 'threw: ' + e.constructor.name }
const kernel = new Worker('/kernel.js')
kernel.onmessage = (e) => { window.__report = { ...report, kernel: e.data } }
</script>`

const server = http.createServer((req, res) => {
  const routes = {
    '/': ['text/html', PAGE],
    '/kernel.js': ['text/javascript', KERNEL],
    '/nested.js': ['text/javascript', NESTED],
  }
  const [type, body] = routes[req.url] ?? ['text/plain', 'not found']
  res.writeHead(routes[req.url] ? 200 : 404, {
    'Content-Type': type,
    'Cross-Origin-Opener-Policy': 'same-origin',
    'Cross-Origin-Embedder-Policy': 'require-corp',
  })
  res.end(body)
})
await new Promise((r) => server.listen(0, '127.0.0.1', r))
const port = server.address().port

const browser = await chromium.launch()
const page = await browser.newPage()
console.log('chromium version:', browser.version())
await page.goto(`http://127.0.0.1:${port}/`)
await page.waitForFunction(() => window.__report !== undefined, { timeout: 15000 })
console.log(JSON.stringify(await page.evaluate(() => window.__report), null, 2))
await browser.close()
server.close()
