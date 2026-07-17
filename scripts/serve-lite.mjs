// Static server for the built JupyterLite site (demos/jupyterlite/_output).
//
// JupyterLite's own `jupyter lite serve` does not send COOP/COEP headers,
// but the worker pool needs the page cross-origin isolated: SharedArrayBuffer
// (used by @fideus-labs/worker-pool and Pyodide's pthread support) and
// nested-worker spawning inside the Pyodide kernel worker both require it.
// The header values mirror vite.config.ts so the JupyterLite demo runs under
// the same isolation regime as the Vite demo (web/).
//
// Usage: node scripts/serve-lite.mjs [--port <n>]   (or PORT env var)
import { createServer } from 'node:http'
import { stat, readFile } from 'node:fs/promises'
import { extname, join, normalize, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = resolve(fileURLToPath(new URL('.', import.meta.url)), '../demos/jupyterlite/_output')

const portFlag = process.argv.indexOf('--port')
const port = Number(portFlag !== -1 ? process.argv[portFlag + 1] : (process.env.PORT ?? 8000))

const crossOriginIsolationHeaders = {
  'Cross-Origin-Opener-Policy': 'same-origin',
  'Cross-Origin-Embedder-Policy': 'require-corp',
}

const mimeTypes = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.ipynb': 'application/json; charset=utf-8',
  '.map': 'application/json; charset=utf-8',
  '.wasm': 'application/wasm',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.gif': 'image/gif',
  '.ico': 'image/x-icon',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.ttf': 'font/ttf',
  '.eot': 'application/vnd.ms-fontobject',
  '.txt': 'text/plain; charset=utf-8',
  '.md': 'text/markdown; charset=utf-8',
  '.py': 'text/x-python; charset=utf-8',
  '.whl': 'application/octet-stream',
  '.zip': 'application/zip',
}

const server = createServer(async (req, res) => {
  const url = new URL(req.url, `http://${req.headers.host}`)
  let pathname = decodeURIComponent(url.pathname)
  if (pathname.endsWith('/')) pathname += 'index.html'

  // Contain resolution inside root (normalize strips any ../ traversal).
  const filePath = join(root, normalize(join('/', pathname)))

  const headers = {
    ...crossOriginIsolationHeaders,
    'Cache-Control': 'no-cache',
  }

  try {
    let target = filePath
    if ((await stat(target)).isDirectory()) {
      target = join(target, 'index.html')
      await stat(target)
    }
    const body = await readFile(target)
    res.writeHead(200, {
      ...headers,
      'Content-Type': mimeTypes[extname(target).toLowerCase()] ?? 'application/octet-stream',
      'Content-Length': body.byteLength,
    })
    res.end(body)
  } catch {
    res.writeHead(404, { ...headers, 'Content-Type': 'text/plain; charset=utf-8' })
    res.end(`404 Not Found: ${pathname}\nDid you run \`npm run build:lite\` first?\n`)
  }
})

server.listen(port, () => {
  console.log(`JupyterLite (cross-origin isolated) at http://localhost:${port}/`)
  console.log(`Serving ${root}`)
})
