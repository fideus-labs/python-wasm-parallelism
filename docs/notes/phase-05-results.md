---
type: note
title: Phase 05 Results — JupyterLite Demos and Smoke Test
created: 2026-07-17
tags:
  - jupyterlite
  - demo
  - results
related:
  - '[[phase-02-results]]'
  - '[[phase-04-results]]'
  - '[[browser-benchmarks]]'
---

# Phase 05 Results — JupyterLite Demos and Smoke Test

Phase 05 put the pool where a Python user actually meets it: a JupyterLite
site (`demos/jupyterlite/`) whose Pyodide kernel — itself a Web Worker —
spawns the pool as **nested workers**, driven from notebooks via a pure
`pyodide_pool` wheel and `await create_pool(...)`. Three demo notebooks ship
with the site (pool basics, the [[phase-02-results]] dask scheduler
in-notebook, and an in-browser benchmark), and a permanent Playwright smoke
test (`e2e/jupyterlite.spec.ts`, project `jupyterlite`) runs
`01-pool-basics.ipynb` end-to-end through the real JupyterLab UI on every
`npm run test:browser`. Representative timing inside the notebook: π(2,000,000)
across 8 chunks, serial ≈ 6.0 s vs parallel ≈ 1.8 s on 4 workers — ≈ 3.3×,
consistent with [[browser-benchmarks]].

## Architecture recap

- **Serving** — `scripts/serve-lite.mjs` serves the built `_output` with the
  same COOP/COEP headers as `vite.config.ts`. Cross-origin isolation is
  required twice over: SharedArrayBuffer *and* nested-worker spawning inside
  the kernel worker. COEP `require-corp` never blocked the Pyodide CDN —
  jsDelivr sends `cross-origin-resource-policy: cross-origin`, so no
  `credentialless` fallback or self-hosting was needed.
- **JS side** — `dist/pyodide-pool.browser.js` (esbuild, `src/browser.ts`) is
  fully self-contained: the worker source is embedded via esbuild `define`
  and spawned from a Blob URL, so nested `new Worker()` resolves without any
  bundler at runtime. Copied into `files/assets/` by `build:lite`.
- **Python side** — `python/pyodide_pool` builds as a pure wheel (hatchling)
  into `files/wheels/`. `loader.create_pool()` imports the JS bundle with
  `pyodide.code.run_js("import('<url>')")`, wraps the JS pool in the existing
  `WorkerPool`, and installs it as the package-wide default pool, so
  notebooks never touch JavaScript.

## Workarounds worth remembering

### Dynamic `import()` inside the kernel resolves against the CDN

Path-absolute URLs (`/files/assets/…`) passed to `import()` from kernel
Python resolve against the **referrer module** — `pyodide.asm.js` on
`cdn.jsdelivr.net` — not the site origin, and 404 on the CDN. `loader.py`
resolves URLs with `new URL(url, location.href)` when `location` exists
(Node keeps raw URLs so the vitest loader suite still passes).

### Kernel-machinery wheels must never be mirrored to pool workers

piplite reports wheels served from its bundled index as `source == "pypi"`,
so the package snapshot name-mirrored `ipykernel`/`pyodide-kernel`/`comm`
into pool workers, where micropip resolved their deps against *real* PyPI
and failed (`tornado` has no pure wheel; `pyodide-kernel` isn't on PyPI).
Fixed both generally and specifically: `EXCLUDED_FROM_MIRROR` drops the
JupyterLite kernel machinery, and workers install every mirrored target with
`deps=False` — the snapshot is transitively complete, so worker-side
resolution is redundant and only ever harmful.

### Notebook install cells use absolute wheel paths

The kernel worker resolves relative URLs against its own script URL under
`extensions/…/static/`, so `piplite.install("wheels/…")` 404s. The first
cell of every notebook installs `/files/wheels/pyodide_pool-<version>-py3-none-any.whl`.

## The permanent smoke test

`e2e/jupyterlite.spec.ts` is a separate Playwright project with its own
`webServer` entry (`npm run build:lite && npm run serve:lite`, port 8000);
`playwright.config.ts` holds both servers as an array, so the whole suite is
one `npm run test:browser`. The test opens
`/lab/index.html?path=01-pool-basics.ipynb`, waits for the kernel to reach
`idle`, drives **Run → Run All Cells** through the real menu bar, and then
asserts on the notebook *model*, not the DOM:

- **`exposeAppInBrowser`** — `demos/jupyterlite/jupyter-lite.json` sets this
  page-config flag so `window.jupyterapp` exists; the test reads kernel
  status and the notebook model (`context.model.toJSON()`) through it.
- **Model over DOM** — JupyterLab 4 virtualizes cell rendering; off-screen
  outputs are simply absent from the DOM, so DOM sweeps can't prove "no cell
  errored". The model always holds every cell's outputs. The test still
  scrolls to the bottom and checks the rendered `POOL_DEMO_OK <speedup>`
  line as a UI-level confirmation (scoped to `.jp-OutputArea-output` — the
  cell's own source code also contains the marker string).
- **Success marker as completeness proof** — the last cell asserts
  `parallel < serial` before printing `POOL_DEMO_OK <speedup>`, and the
  notebook's cells form a dependency chain, so the marker cannot print
  unless every cell ran; execution counts are deliberately *not* asserted
  (next item).

### Upstream race: coincident's `Atomics.waitAsync` 'not-equal'

The one recurring flake was a page error
`….value.then is not a function` from `jupyterlite-pyodide-kernel`'s
vendored **coincident** transport (SharedArrayBuffer RPC between kernel
worker and main thread): when the notify lands before the wait starts,
`Atomics.waitAsync` returns `{ value: 'not-equal' }` — a string, not a
promise — and coincident calls `.value.then(...)` on it. The throw drops one
in-flight kernel message; observed effect was a single cell with an empty
execution-count prompt in an otherwise complete, correct run. The demo
outcome is unaffected, so the test allow-lists exactly this error pattern
(and the shared console-watchdog fixture now applies its allow-list to
`pageerror` events too, which it previously never filtered).

## Verification status

- `npm run test:browser` — 7/7 (6 Vite-demo tests + the JupyterLite smoke
  test); the smoke test passed 3× consecutively at ~20 s each.
- `npx vitest run` — 62/62; `npm run typecheck` clean; `npm run check:wheel`
  9/9.
