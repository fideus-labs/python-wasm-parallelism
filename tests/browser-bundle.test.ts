/**
 * The self-contained browser bundle (dist/pyodide-pool.browser.js): built
 * from real sources via the canonical scripts/bundles.mjs definition, then
 * verified two ways — as text (self-containment is what JupyterLite's
 * kernel worker depends on: no static imports left to resolve) and as a
 * real dynamically-imported module (the exact way loader.py consumes it).
 */
import { readFileSync } from 'node:fs'
import { createRequire } from 'node:module'
import { pathToFileURL } from 'node:url'
import { beforeAll, describe, expect, it } from 'vitest'
import { buildBrowserBundle } from '../scripts/bundles.mjs'

type BrowserBundle = typeof import('../src/browser.js')

let bundleText: string
let bundle: BrowserBundle

beforeAll(async () => {
  const { outfile } = await buildBrowserBundle()
  bundleText = readFileSync(outfile, 'utf8')
  bundle = (await import(pathToFileURL(outfile).href)) as BrowserBundle
})

describe('self-containment', () => {
  it('has no static imports left — every specifier is bundled', () => {
    // esbuild emits externals it cannot bundle as top-level `import ... from`
    // statements; a fully inlined ESM bundle starts no line with one. (The
    // package name still appears in esbuild's provenance comments — only an
    // import of it would break self-containment.)
    expect(bundleText).not.toMatch(/^import[\s{"']/m)
    expect(bundleText).not.toMatch(/import\(["']@fideus-labs\/worker-pool["']\)/)
  })

  it('keeps pyodide and web-worker as dynamic-only imports', () => {
    // Never executed in browsers: workers get a CDN moduleURL on every
    // request, and `Worker` exists globally so the polyfill path is dead.
    expect(bundleText).toContain('import("pyodide")')
    expect(bundleText).toContain('import("web-worker")')
  })

  it('embeds the worker bundle source', () => {
    // 'Malformed request' only occurs in the worker's dispatch() — its
    // presence (JSON-escaped, inside the embedded string) proves the worker
    // code travels within the single file.
    expect(bundleText).toContain('Malformed request')
  })
})

describe('module surface', () => {
  it('re-exports the library API', () => {
    expect(typeof bundle.PyodidePool).toBe('function')
    expect(typeof bundle.PyodideTaskError).toBe('function')
  })

  it('pins the CDN default to the installed pyodide version', () => {
    const { version } = createRequire(import.meta.url)('pyodide/package.json') as {
      version: string
    }
    expect(bundle.PYODIDE_VERSION).toBe(version)
    expect(bundle.DEFAULT_PYODIDE_SOURCE.moduleURL).toBe(
      `https://cdn.jsdelivr.net/pyodide/v${version}/full/pyodide.mjs`,
    )
    expect(bundle.DEFAULT_PYODIDE_SOURCE.indexURL).toBe(
      `https://cdn.jsdelivr.net/pyodide/v${version}/full/`,
    )
  })

  it('serves the inlined worker from a stable Blob URL', () => {
    const url = bundle.workerBlobUrl()
    expect(url).toMatch(/^blob:/)
    expect(bundle.workerBlobUrl()).toBe(url)
  })

  it('createPool wires defaults without spawning workers', () => {
    const pool = bundle.createPool()
    expect(pool).toBeInstanceOf(bundle.PyodidePool)
    expect(pool.poolSize).toBe(4)
    expect(bundle.createPool({ poolSize: 2 }).poolSize).toBe(2)
  })

  it('createPool rejects a non-positive pool size', () => {
    expect(() => bundle.createPool({ poolSize: 0 })).toThrow(RangeError)
  })
})
