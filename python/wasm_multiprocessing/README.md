# 🧩 wasm-multiprocessing

`multiprocessing.Pool` for [Pyodide](https://pyodide.org): same API, Web
Workers underneath.

🔗 [Live Demo](https://fideus-labs.github.io/python-wasm-parallelism/) ·
[Source](https://github.com/fideus-labs/python-wasm-parallelism)

Stdlib `multiprocessing` cannot run under WebAssembly (no `fork`, no
`-pthread`). `wasm-multiprocessing` provides stdlib-shaped
`Pool`/`AsyncResult` over a pool of Pyodide Web Workers, so existing
`Pool`-based code runs after changing one import line, within the limits
below.

## Usage

```python
import pyodide_pool

await pyodide_pool.create_pool(pool_size=4)  # start the worker pool

# was: from multiprocessing import Pool
from wasm_multiprocessing import Pool

with Pool(4) as pool:
    counts = await pool.amap(count_primes, ranges)  # portable everywhere
    # or, where JSPI is available, block for real:
    # counts = pool.map(count_primes, ranges)
```

- **Async-native everywhere** — `amap`, `astarmap`, `aapply`, … .
- **Blocking where safe** — `map`/`starmap`/`apply`/`join` and
  `AsyncResult.get`/`wait` truly block where JSPI stack switching is
  available; elsewhere they raise a `RuntimeError` naming the async
  replacement.
- **Honest limits** — fork-shaped APIs (`Process`, `Queue`, `Manager`,
  shared memory, locks) raise `NotImplementedError`: WebAssembly workers
  share nothing.

Built on [`pyodide-pool`](https://github.com/fideus-labs/python-wasm-parallelism/tree/main/python),
which handles cloudpickle transport, package mirroring, and the JS bridge.
Like `pyodide-pool`, it runs only in the driver Pyodide instance.

## Documentation

- [Project README](https://github.com/fideus-labs/python-wasm-parallelism#readme)
- [multiprocessing shim design](https://github.com/fideus-labs/python-wasm-parallelism/blob/main/docs/architecture/multiprocessing-shim-design.md)
  — full API mapping
- [dask vs multiprocessing](https://github.com/fideus-labs/python-wasm-parallelism/blob/main/docs/architecture/dask-vs-multiprocessing.md)

## License

MIT
