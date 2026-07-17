# Pyodide Worker-Pool Demos

Demo notebooks for running CPU-bound Python in parallel from inside
JupyterLite. The Pyodide kernel itself runs in a Web Worker; the pool
spawns nested workers, each with its own Pyodide interpreter.

Requirements (provided by the bundled `serve:lite` server):

- Cross-origin isolation (COOP/COEP headers) for `SharedArrayBuffer`
- A browser that supports workers spawning nested workers

Notebooks (added in later phases):

- `01-pool-basics.ipynb` — pool creation, `submit`, `map`, serial vs parallel timing
- `02-dask-parallel.ipynb` — `dask.delayed` / `dask.bag` graphs computed on the pool
- `03-benchmark.ipynb` — parameterized workers × workload benchmark with charts
