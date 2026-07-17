---
type: note
title: Phase 04 Results — Browser Demo, Playwright Tests, and Benchmarks
created: 2026-07-17
tags:
  - browser
  - playwright
  - results
related:
  - '[[browser-benchmarks]]'
  - '[[node-benchmarks]]'
  - '[[phase-02-results]]'
---

# Phase 04 Results — Browser Demo, Playwright Tests, and Benchmarks

Phase 04 brought the Phase 01 pool and Phase 02 dask scheduler to their real
target: headless Chromium serving the production Vite bundle of the `web/`
demo. The e2e suite (`npm run test:browser`) is 6/6 green (twice
consecutively), and the `@bench` spec produced [[browser-benchmarks]] —
browser throughput is within 4% of the Node numbers in [[node-benchmarks]]
once workers are warm. This note records the browser-specific workarounds
the phase needed; the benchmark analysis itself lives in the reports.

## Workarounds and environment-specific decisions

### Pyodide loading: one new knob instead of a browser fork

`src/` ran in the browser unchanged except for a single addition: workers
cannot resolve `node_modules` in a browser, so every protocol request
(`exec`/`execPickled`/`ping`) gained an optional
`PyodideSource { moduleURL, indexURL }`, set pool-wide via
`PyodidePoolOptions.pyodideSource`. The worker dynamically imports
`loadPyodide` from `moduleURL` when present (the browser passes the jsDelivr
CDN, pinned to the installed npm version 314.0.2 so Node and browser test
the same runtime) and falls back to the bare `pyodide` import (Node).
Covered by a Node test using a `file://` moduleURL.

### Cross-origin isolation: `require-corp` suffices with jsDelivr

SharedArrayBuffer needs COOP `same-origin` + COEP on both the Vite dev and
preview servers. The anticipated fallback to `credentialless` (in case
`require-corp` blocked CDN responses) was **not** needed: jsDelivr serves
Pyodide assets with CORP/CORS headers that satisfy `require-corp`, and
`crossOriginIsolated === true` held in dev and preview with the pool booting
from the CDN. Keep `require-corp` — it is the stricter, Safari-compatible
setting.

### Vite worker bundling

The worker is bundled with the `?worker&url` suffix (module workers must be
ES format, configured under `worker.format`), with `pyodide` marked external
in the worker build — it is dynamically imported from the CDN at runtime,
never bundled. The driver-side Python package sources (`python/pyodide_pool`)
ship into the page via `?raw` imports. No dev-vs-build `import.meta.url`
drift appeared: the production bundle ran identically to dev.

### ARM64 WSL2 tooling: plain `playwright install chromium`, no sudo

This host is Linux ARM64 (WSL2). `npx playwright install chromium
--with-deps` fails (no passwordless sudo), but plain
`npx playwright install chromium` works and the browser launches with no
missing system libraries. Browser-automation MCP servers (Playwright MCP
hard-configured for the `chrome` channel, chrome-devtools MCP) cannot launch
here; interactive smoke-testing used the agent-browser CLI pointed at
Playwright's ARM64 Chromium instead. The Playwright test suite itself is
unaffected.

### Playwright configuration choices

- `webServer` runs `npm run build:web && npm run preview -- --strictPort`:
  every run tests a freshly built production bundle, and `--strictPort`
  fails loudly if :4173 is already taken.
- `workers: 1` + `fullyParallel: false` — each page boots a main Pyodide
  plus a worker pool; parallel test files would contend for CPU and skew
  `@bench` timings.
- The `@bench` exclusion lives in the npm script
  (`playwright test --grep-invert @bench`), not in the config: a
  config-level `grepInvert` combines with a CLI `--grep @bench` and would
  match nothing.
- Test timeout 240 s / `expect` timeout 120 s absorb CDN Pyodide boots;
  action/navigation timeouts stay tight (15 s/30 s).

### Test-suite structural details

- The console-error fixture is `{ auto: true }` and also captures
  `pageerror` — that is what makes "a Python exception surfaces as a
  readable UI error, **not** an unhandled rejection" a real assertion.
  Allow-list: only Pyodide's `Loading …`/`Loaded …` package-mirroring lines.
- `<progress>` is property-driven; its attributes never update, so tests
  assert the live `value === max`, not attribute values.
- All assertions are structural (result equality, progress completion,
  ✓ cells); durations are recorded by `@bench` for the report, never
  asserted.

### Bench schema accommodations for the browser harness

- Overheads the page cannot measure (no raw pool handle behind
  `window.__demo`) use the `{ samples: [], medianMs: NaN }` convention;
  NaN serializes to `null`, so the report generator guards with
  `Number.isFinite`, not just the type.
- `BenchContext` gained optional `browser`/`userAgent`; per-workload configs
  became optional with `NodeBenchConfig = Required<BenchConfig>` for the
  Node harness. The serial baseline is recorded as `poolSize: 1`.
- `window.__demo.setup()` exposes per-pool `warmup()` wall/boot times so the
  results JSON gets honest `setup[]`/`workerBoot` numbers.
- `__demo` types live in Vite-free `web/demo-api.ts` so the root (NodeNext)
  tsconfig shares them with `e2e/` without loading Vite client types.

### Report generation shared between Node and browser

The Node report generator was refactored, not copied: shared blocks
(front matter, workload tables, scaling sentences, warning callouts) moved
to `bench/report-common.ts`, consumed by both `bench/report.ts` (Node) and
`bench/report-browser.ts` (+ `bench/report-browser-cli.ts`, wired as
`npm run bench:report:browser`). Regenerating [[node-benchmarks]] with the
refactored generator reproduces the committed file byte-for-byte.
