/**
 * Types for the `window.__demo` hook shared between the demo app
 * (web/main.ts) and the Playwright suite (e2e/). Pure type declarations —
 * no imports and no Vite-specific syntax — so the root tsconfig (NodeNext,
 * which type-checks e2e/) can consume them without tripping over the
 * `?raw`/`?worker&url` modules in web/main.ts.
 */

export interface RunRecord {
  kind: 'serial' | 'parallel'
  label: string
  workers: number
  /** Wall-clock of the workload run (pool boots excluded). */
  ms: number
  /** Per-chunk prime counts, in chunk order. */
  counts: number[]
  total: number
  expectedTotal: number
  matchesExpected: boolean
  /** Counts identical to the latest serial baseline; null without one. */
  matchesSerial: boolean | null
  /** vs the latest serial baseline; null without one (or for the baseline). */
  speedup: number | null
}

export interface DaskRecord {
  kind: 'dask'
  label: string
  workers: number
  /** Wall-clock of pyodide_pool.compute (the pool scheduler). */
  ms: number
  /** Wall-clock of dask's synchronous scheduler on the same graph. */
  syncMs: number
  poolTotal: number
  syncTotal: number
  matchesExpected: boolean
  /** Pool-scheduler result equals the synchronous-scheduler result. */
  equal: boolean
  speedup: number
}

export type DemoRecord = RunRecord | DaskRecord

/** Warmup cost of one pool, recorded when `poolOfSize` first boots it. */
export interface PoolSetupStat {
  poolSize: number
  /** Wall-clock of pool.warmup() — boots poolSize interpreters in parallel. */
  warmupWallMs: number
  /** Per-worker cold Pyodide boot times reported by warmup(). */
  workerBootMs: number[]
  /** Pyodide runtime version reported by the workers' post-boot ping. */
  pyodideVersion: string | null
}

export interface DemoApi {
  /** Resolves with 'ready' once the driver has booted and the pool is warm. */
  ready: Promise<string>
  isolated: boolean
  config: {
    poolSize: number
    rangeStart: number
    rangeEnd: number
    chunkCount: number
    expectedTotal: number
    pyodideVersion: string
  }
  /** All finished runs, oldest first. */
  results(): DemoRecord[]
  /** Per-pool-size warmup costs, in boot order (the @bench spec's `setup`). */
  setup(): PoolSetupStat[]
  runSerial(): Promise<RunRecord>
  runParallel(workers?: number): Promise<RunRecord>
  runDask(): Promise<DaskRecord>
  /** Dask graph whose tasks need numpy — exercises CDN package mirroring. */
  runNumpy(): Promise<DaskRecord>
  /** Trigger a Python failure; resolves with the UI-surfaced message. */
  runFailing(): Promise<string>
}

declare global {
  interface Window {
    __demo: DemoApi
  }
}
