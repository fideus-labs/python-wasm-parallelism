# Pyodide Worker-Pool Demos

Demo notebooks for running CPU-bound Python in parallel from inside
JupyterLite. The Pyodide kernel itself runs in a Web Worker; the pool
spawns nested workers, each with its own Pyodide interpreter.

Requirements: a browser that supports workers spawning nested workers, and
network access to the Pyodide CDN and PyPI. The pool itself is
postMessage-based, so it also runs on hosts that cannot send COOP/COEP
headers (GitHub Pages); the bundled `serve:lite` server does send them,
which additionally lights up `SharedArrayBuffer` paths (the kernel's
faster coincident transport).

Notebooks (run them top to bottom; each is self-contained):

- `00-scipy-lightning.ipynb` — the SciPy lightning talk: every Minnesota
  lake, a dask KDE of lake density on the pool, serial-vs-pool speedup,
  and the how/why in two closing cells
- `01-pool-basics.ipynb` — pool creation, `submit`, parallel map, serial vs parallel timing
- `02-dask-parallel.ipynb` — `dask.delayed` / `dask.bag` graphs computed on the pool, package mirroring
- `03-benchmark.ipynb` — parameterized workers × workload benchmark with a results table and speedup chart, closing with a dask-vs-multiprocessing-shim comparison
- `04-multiprocessing.ipynb` — the `multiprocessing.Pool` shim: one-line port of a classic script, `AsyncResult`, JSPI capability detection, `Pool(1)`/`Pool(2)`/`Pool(4)` timings
