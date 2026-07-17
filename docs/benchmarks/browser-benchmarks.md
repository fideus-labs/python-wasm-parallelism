---
type: report
title: Browser Benchmarks
created: 2026-07-17
tags:
  - benchmark
  - browser
  - playwright
related:
  - '[[node-benchmarks]]'
  - '[[phase-02-results]]'
---

# Browser Benchmarks

Serial-vs-parallel performance of the Pyodide worker pool in a real
browser: the `@bench` Playwright spec (e2e/bench.spec.ts, run via
`npx playwright test --grep @bench`) drives the `web/` demo’s
`window.__demo` hook in headless Chromium against the production Vite
bundle. This file is regenerated from `bench/results/browser-2026-07-17.json`
(`npm run bench:report:browser`) — edit the spec or the generator, not
this file. The pool under test is the Phase 01–02 build summarized in
[[phase-02-results]]; the Node counterpart of this report is
[[node-benchmarks]].

## Environment

| | |
| --- | --- |
| Date | 2026-07-17 |
| Browser | chromium 149.0.7827.0 (headless, cross-origin isolated) |
| Pyodide | 314.0.2 (jsDelivr CDN pin) / 314.0.2 (runtime) |
| Test runner | Node v24.18.0 (Playwright; the workloads run in the browser) |
| OS | linux 6.6.114.1-microsoft-standard-WSL2 (arm64) |
| CPU | unknown, 16 logical cores (availableParallelism 16) |
| Pool sizes | 1 (serial), 2, 4 |
| Repetitions | 3 timed (median reported) after 1 untimed warmup run per cell |

## Workloads

Every cell runs the same fixed total work; pools are warmed (interpreters
booted, packages installed/mirrored) before timing, and each cell runs one
additional untimed warmup repetition. Efficiency = speedup ÷ workers.

### Prime counting (pure Python, CPU-bound)

Trial-division prime counting from the browser demo, driven through `window.__demo` in headless Chromium (serial = every chunk sequentially in one task on a warm worker; parallel = `pool.map()` across the chunks). Fixed total work: primes in [2, 2000000) across 8 chunks.

| Workers | Median wall-clock | Speedup | Efficiency |
| --- | --- | --- | --- |
| serial (1) | 6.09 s | 1.00× | 100% |
| 2 | 3.24 s | 1.88× | 94% |
| 4 | 1.95 s | 3.13× | 78% |

## Overheads

| Overhead | Median | Samples |
| --- | --- | --- |
| Per-worker Pyodide boot (jsDelivr CDN) | 1.43 s | 6 cold boots |

No-op and payload round-trip floors are not measurable through
`window.__demo` (the page exposes workload methods, not a raw pool
handle); the results JSON records them with the harness’s
`{ samples: [], medianMs: NaN }` unmeasured convention. See
[[node-benchmarks]] for those dispatch floors — the browser shares the
same postMessage/structured-clone task path.

## Analysis

- **Prime counting (pure Python, CPU-bound)** reaches 3.13× on 4 workers (78% efficiency). Scaling holds through the largest pool measured.

Boot amortization: each worker pays a one-time Pyodide boot of ~1.43 s,
fetched from the jsDelivr CDN (median across 6 cold boots — see the
CDN cost note below). Warmup boots interpreters in parallel, so the
recorded pool warmups cost 1.48 s (4 workers) and 1.18 s (2 workers) of wall-clock,
and the serial baseline runs on an already-warm worker — all timings
above exclude boot, exactly as in the Node harness.

## Node vs browser

Same machine and the same fixed total work per workload; the Node run is
`bench/results/node-2026-07-17.json` ([[node-benchmarks]]). Ratios are browser median ÷ Node
median (1.00× = parity).

### Prime counting (pure Python, CPU-bound)

| Workers | Node median | Browser median | Browser ÷ Node |
| --- | --- | --- | --- |
| serial (1) | 5.86 s | 6.09 s | 1.04× |
| 2 | 3.27 s | 3.24 s | 0.99× |
| 4 | 1.96 s | 1.95 s | 0.99× |

Node's matrix additionally measures 8 workers → 1.23 s; the browser
demo caps its pool at the sizes above.

CDN package-load costs are the structural difference: a browser worker
boots Pyodide from the jsDelivr CDN — runtime JS, `pyodide.asm.wasm`,
and the stdlib bundle over HTTP — where Node reads `node_modules` from
local disk, so the per-worker boot is 1.43 s vs 1.11 s
(+29%) and, unlike Node's, depends on network and HTTP-cache state.
The prime-counting workload mirrors no packages, so the CDN surcharge
appears only in boot here; first-touch package mirroring (the demo’s
dask and numpy graphs) additionally fetches wheels from PyPI/the Pyodide
CDN at first use — the first-task-pays pattern measured in
[[phase-02-results]], with the network fetch added on top.

Once workers are warm the gap nearly closes: every shared cell above is
within 4% of Node. The compute is the same single-threaded
WebAssembly interpreter in both environments; the residual constant is
browser-side scheduling and messaging overhead, not the workload.
