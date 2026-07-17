/**
 * Spike 1 — Node main thread: what does Atomics.wait actually permit?
 *
 * Questions (design doc: multiprocessing-shim-design):
 *  A. Is Atomics.wait legal on the Node main thread?                (expect: yes)
 *  B. While the main thread is blocked in Atomics.wait, can a
 *     worker_threads worker write a result DIRECTLY into a SAB and
 *     Atomics.notify the blocked thread awake?                      (expect: yes)
 *  C. While the main thread is blocked, are worker postMessage
 *     responses delivered? (i.e. could the EXISTING promise-based
 *     pool protocol complete under a blocked driver?)               (expect: NO)
 *  D. While the main thread is blocked, do pending microtasks /
 *     async-function continuations run? (pool dispatch is an async
 *     chain — postMessage happens in a microtask)                   (expect: NO)
 */
import { Worker } from 'node:worker_threads'

const results = {}

// --- A: bare legality -------------------------------------------------------
{
  const sab = new SharedArrayBuffer(4)
  const ia = new Int32Array(sab)
  const t0 = Date.now()
  const outcome = Atomics.wait(ia, 0, 0, 50) // nobody notifies: expect 'timed-out'
  results.A = { outcome, elapsedMs: Date.now() - t0 }
}

// --- worker used by B, C ----------------------------------------------------
const workerSource = `
import { parentPort, workerData } from 'node:worker_threads'
const ia = new Int32Array(workerData.sab)
parentPort.on('message', (msg) => {
  if (msg === 'sab-result') {
    // simulate computing then writing the result directly into shared memory
    setTimeout(() => {
      Atomics.store(ia, 1, 42)        // "result"
      Atomics.store(ia, 0, 1)         // "ready" flag
      Atomics.notify(ia, 0)
    }, 100)
  } else if (msg === 'post-result') {
    setTimeout(() => parentPort.postMessage('the-result'), 100)
  }
})
`
const sab = new SharedArrayBuffer(8)
const ia = new Int32Array(sab)
const worker = new Worker(workerSource, { eval: true, workerData: { sab } })
await new Promise((r) => worker.once('online', r))

// --- B: SAB mailbox while blocked ------------------------------------------
{
  Atomics.store(ia, 0, 0)
  worker.postMessage('sab-result')
  const t0 = Date.now()
  const outcome = Atomics.wait(ia, 0, 0, 5000) // block up to 5 s
  results.B = {
    outcome, // 'ok' means the worker's notify woke us
    elapsedMs: Date.now() - t0,
    value: Atomics.load(ia, 1),
  }
}

// --- C: postMessage delivery while blocked ----------------------------------
{
  let delivered = null
  worker.on('message', (m) => { delivered = m })
  worker.postMessage('post-result')
  const gate = new Int32Array(new SharedArrayBuffer(4))
  const t0 = Date.now()
  Atomics.wait(gate, 0, 0, 1000) // block 1 s; worker replies after 100 ms
  results.C = {
    deliveredWhileBlocked: delivered, // expect null — event loop never turned
    elapsedMs: Date.now() - t0,
  }
  // let the loop turn once, message should now arrive
  await new Promise((r) => setTimeout(r, 50))
  results.C.deliveredAfterUnblock = delivered
}

// --- D: microtask starvation -------------------------------------------------
{
  let ran = false
  Promise.resolve().then(() => { ran = true }) // queued microtask
  const gate = new Int32Array(new SharedArrayBuffer(4))
  Atomics.wait(gate, 0, 0, 300)
  results.D = { microtaskRanWhileBlocked: ran } // expect false
  await Promise.resolve()
  results.D.microtaskRanAfterUnblock = ran
}

console.log(JSON.stringify(results, null, 2))
await worker.terminate()
