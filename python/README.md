# 🐍⚡ pyodide-pool

Run Python callables and dask graphs in parallel on a pool of
[Pyodide](https://pyodide.org) Web Workers.

🔗 [Live Demo](https://fideus-labs.github.io/python-wasm-parallelism/) ·
[Source](https://github.com/fideus-labs/python-wasm-parallelism)

A single Pyodide interpreter is strictly single-threaded (no `-pthread`, no
`fork`), so parallelism means *many interpreters*. `pyodide-pool` is the
driver-side Python API for a pool of Web Workers (browser) or
`worker_threads` (Node), each hosting its own Pyodide interpreter:

- **`submit`** — run one cloudpickled call on a worker and await its result.
- **`compute` / `get`** — an async dask scheduler that executes
  `dask.delayed` / `dask.bag` / dask-array graphs in parallel on the pool.
- **Package mirroring** — packages loaded in the driver (e.g. numpy) are
  installed on each worker on first use.
- **Remote errors** — the original Python exception is re-raised on the
  driver with the worker traceback chained.

The package runs only in the driver Pyodide instance; workers never install
it. It is async-first: blocking the driver would starve the event loop the
pool lives on.

## Usage

```python
import dask
import pyodide_pool

# In JupyterLite: import the JS bundle and start 4 nested workers
pool = await pyodide_pool.create_pool(pool_size=4)

value = await pyodide_pool.submit(fn, *args, **kwargs)  # one remote call
total = await pyodide_pool.compute(dask.delayed(sum)([...]))  # dask collections
```

`create_pool` loads the self-contained JS bundle from the JupyterLite site by
default; pass `js_url=` to load it from elsewhere. A web app that embeds
Pyodide can instead create the JS `PyodidePool` itself and register it with
`pyodide.registerJsModule('js_pyodide_pool', { pool })`.

Install `pyodide-pool[dask]` for `compute` / `get`.

For the multiprocessing-style API, see
[`wasm-multiprocessing`](https://github.com/fideus-labs/python-wasm-parallelism/tree/main/python/wasm_multiprocessing).

## Documentation

- [Project README](https://github.com/fideus-labs/python-wasm-parallelism#readme)
- [dask scheduler design](https://github.com/fideus-labs/python-wasm-parallelism/blob/main/docs/architecture/dask-scheduler-design.md)
- [Benchmarks](https://github.com/fideus-labs/python-wasm-parallelism/tree/main/docs/benchmarks)

## License

MIT
