// Build the JupyterLite payload, run by `npm run build:lite` ahead of
// `jupyter lite build`:
//
//   1. the self-contained browser bundle -> demos/jupyterlite/files/assets/
//      (import()ed from inside the Pyodide kernel worker by
//      python/pyodide_pool/loader.py)
//   2. the pyodide_pool wheel (python/pyproject.toml, built with uv)
//      -> demos/jupyterlite/files/wheels/ (installed by the notebooks'
//      first piplite cell)
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
try {
  execFileSync('uv', ['build', '--wheel', '--out-dir', wheelsDir, path.join(rootDir, 'python')], {
    stdio: 'inherit',
  })
} catch (err) {
  if (err.code === 'ENOENT') {
    console.error(
      'uv not found on PATH — install uv (https://docs.astral.sh/uv/) or build the wheel ' +
        'manually: python -m build --wheel --outdir demos/jupyterlite/files/wheels python',
    )
    process.exit(1)
  }
  throw err
}
// uv drops a catch-all .gitignore into --out-dir; the repo ignores the wheel
// dir itself, and the stray file would otherwise ship into the built site.
rmSync(path.join(wheelsDir, '.gitignore'), { force: true })
