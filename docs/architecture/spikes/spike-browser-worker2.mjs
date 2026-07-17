/**
 * Spike 3b — refinement of spike-browser-worker.mjs.
 *
 * 3a found the nested worker NEVER woke its blocked parent, and nothing
 * arrived even after unblock — hypothesis: a nested worker cannot finish
 * BOOTING while its parent worker is blocked (script fetch/lifecycle runs on
 * the parent's loop). Distinguish:
 *  C1. handshake-boot the nested worker FIRST, then block the parent and ask
 *      for a SAB write + notify           → does a WARM nested worker wake it?
 *  C2. same but nested worker created immediately before blocking (cold)
 *  D.  warm nested worker postMessage during parent block → still starved?
 */
import http from 'node:http'
import { chromium } from '@playwright/test'

const NESTED = `
onmessage = (e) => {
  if (e.data.mode === 'ping') { postMessage('pong'); return }
  const ia = new Int32Array(e.data.sab)
  if (e.data.mode === 'sab') {
    setTimeout(() => { Atomics.store(ia, 1, 77); Atomics.store(ia, 0, 1); Atomics.notify(ia, 0) }, 100)
  } else {
    setTimeout(() => postMessage('nested-result'), 100)
  }
}
`

const KERNEL = `
const report = {}
const warm = new Worker('/nested.js')
warm.onmessage = () => {   // 'pong' — the warm worker is fully booted
  // C1: WARM nested worker, parent blocks, worker writes SAB + notifies
  {
    const sab = new SharedArrayBuffer(8)
    const ia = new Int32Array(sab)
    warm.postMessage({ mode: 'sab', sab })
    const t0 = Date.now()
    report.warmSabWake = { outcome: Atomics.wait(ia, 0, 0, 5000), elapsedMs: Date.now() - t0, value: Atomics.load(ia, 1) }
  }
  // D: warm nested worker postMessage while parent blocks
  {
    let delivered = null
    warm.onmessage = (e) => { delivered = e.data }
    warm.postMessage({ mode: 'post' })
    const gate = new Int32Array(new SharedArrayBuffer(4))
    Atomics.wait(gate, 0, 0, 1000)
    report.warmPostWhileBlocked = delivered
  }
  // C2: COLD nested worker created right before the parent blocks
  {
    const cold = new Worker('/nested.js')
    const sab = new SharedArrayBuffer(8)
    const ia = new Int32Array(sab)
    cold.postMessage({ mode: 'sab', sab })
    const t0 = Date.now()
    report.coldSabWake = { outcome: Atomics.wait(ia, 0, 0, 3000), elapsedMs: Date.now() - t0, value: Atomics.load(ia, 1) }
  }
  setTimeout(() => postMessage(report), 300)
}
warm.postMessage({ mode: 'ping' })
`

const PAGE = `<!doctype html><script>
const kernel = new Worker('/kernel.js')
kernel.onmessage = (e) => { window.__report = e.data }
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
await page.goto(`http://127.0.0.1:${port}/`)
await page.waitForFunction(() => window.__report !== undefined, { timeout: 30000 })
console.log(JSON.stringify(await page.evaluate(() => window.__report), null, 2))
await browser.close()
server.close()
