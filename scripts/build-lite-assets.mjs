// Build the JupyterLite payload, run by `npm run build:lite` ahead of
// `jupyter lite build`:
//
//   1. the self-contained browser bundle -> demos/jupyterlite/files/assets/
//      (import()ed from inside the Pyodide kernel worker by
//      python/pyodide_pool/loader.py)
//   2. the pyodide_pool and wasm_multiprocessing wheels (python/pyproject.toml
//      and python/wasm_multiprocessing/pyproject.toml, built with uv)
//      -> demos/jupyterlite/files/wheels/ (installed by the notebooks'
//      first piplite cell; wasm_multiprocessing depends on pyodide_pool,
//      which is not on PyPI, so the notebooks install the pool wheel first)
//
// Both output directories are generated (gitignored); this script is the
// only writer.
import { execFileSync } from 'node:child_process'
import { copyFileSync, mkdirSync, rmSync } from 'node:fs'
import path from 'node:path'
import { buildBrowserBundle, rootDir } from './bundles.mjs'

const liteFiles = path.join(rootDir, 'demos', 'jupyterlite', 'files')

const { outfile } = await buildBrowserBundle({ logLevel: 'info' })
const assetsDir = path.join(liteFiles, 'assets')
mkdirSync(assetsDir, { recursive: true })
copyFileSync(outfile, path.join(assetsDir, path.basename(outfile)))
console.log(`copied ${path.basename(outfile)} -> demos/jupyterlite/files/assets/`)

const wheelsDir = path.join(liteFiles, 'wheels')
mkdirSync(wheelsDir, { recursive: true })
const wheelProjects = [path.join(rootDir, 'python'), path.join(rootDir, 'python', 'wasm_multiprocessing')]
try {
  for (const project of wheelProjects) {
    execFileSync('uv', ['build', '--wheel', '--out-dir', wheelsDir, project], {
      stdio: 'inherit',
    })
  }
} catch (err) {
  if (err.code === 'ENOENT') {
    console.error(
      'uv not found on PATH — install uv (https://docs.astral.sh/uv/) or build the wheels ' +
        'manually: python -m build --wheel --outdir demos/jupyterlite/files/wheels ' +
        wheelProjects.join(' '),
    )
    process.exit(1)
  }
  throw err
}
// uv drops a catch-all .gitignore into --out-dir; the repo ignores the wheel
// dir itself, and the stray file would otherwise ship into the built site.
rmSync(path.join(wheelsDir, '.gitignore'), { force: true })
