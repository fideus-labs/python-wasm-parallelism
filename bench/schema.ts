/**
 * Machine-readable result schema shared by the benchmark producers
 * (bench/run-bench.ts for Node, e2e/bench.spec.ts for the browser) and the
 * report generator (bench/report.ts, consumer).
 * docs/benchmarks/node-benchmarks.md is always REGENERATED from a
 * bench/results/node-<date>.json conforming to these types — never edited by
 * hand — so schema changes must update the generator in the same commit.
 */

export interface BenchContext {
  /** Node version — for browser runs, the Playwright runner's Node. */
  node: string
  /** Version of the pyodide npm package (node_modules/pyodide or CDN pin). */
  pyodideNpm: string | null
  /** Pyodide runtime version reported by a worker ping after boot. */
  pyodideRuntime: string | null
  platform: string
  release: string
  arch: string
  cpuModel: string
  cores: number
  availableParallelism: number
  /** Browser name + version — present only for browser (@bench spec) runs. */
  browser?: string
  /** navigator.userAgent — present only for browser runs. */
  userAgent?: string
}

/** Prime-counting range config (shared by the raw-pool and dask workloads). */
export interface PrimesConfig {
  rangeStart: number
  rangeEnd: number
  chunkCount: number
  /** π(rangeEnd) when known — verified against the computed total. */
  expectedTotal: number | null
}

export interface PiConfig {
  chunkCount: number
  samplesPerChunk: number
}

export interface MatmulConfig {
  taskCount: number
  /** Square matrix dimension. */
  n: number
  /** Chained matmuls per task. */
  k: number
}

export interface BenchConfig {
  /** Pool sizes in run order; 1 is the serial baseline and always first. */
  poolSizes: number[]
  /** Timed repetitions per (workload, poolSize) cell; median is reported. */
  repetitions: number
  primes: PrimesConfig
  /**
   * Workload/overhead configs below are absent when a harness doesn't run
   * them — the browser @bench spec (e2e/bench.spec.ts) records only primes.
   */
  pi?: PiConfig
  matmul?: MatmulConfig
  dask?: PrimesConfig
  noopReps?: number
  payloadReps?: number
  /** float64 element count of the echo payload (bytes = 8 × this). */
  payloadFloats?: number
}

/** The Node harness (bench/run-bench.ts) runs every workload and overhead. */
export type NodeBenchConfig = Required<BenchConfig>

export interface BenchCell {
  poolSize: number
  /** Untimed first run; absorbs first-touch costs (package installs, JIT). */
  warmupMs: number
  runsMs: number[]
  medianMs: number
  /** Workload verification value — must agree across reps and pool sizes. */
  value: number
}

export interface WorkloadResult {
  id: string
  title: string
  description: string
  /** Human description of the fixed total work (identical for every cell). */
  totalWork: string
  cells: BenchCell[]
}

export interface OverheadStat {
  samples: number[]
  medianMs: number
}

/** Per-pool-size setup costs (never counted in workload timings). */
export interface PoolSetup {
  poolSize: number
  /** Wall-clock of pool.warmup() — boots poolSize interpreters in parallel. */
  warmupWallMs: number
  /** Per-worker cold Pyodide boot times reported by warmup(). */
  workerBootMs: number[]
  /** Driver-side package-mirroring warm-up (dask/numpy replay to workers). */
  mirrorWarmMs: number
}

export interface BenchResults {
  schema: 1
  createdAt: string
  mode: 'full' | 'smoke'
  context: BenchContext
  config: BenchConfig
  setup: PoolSetup[]
  workloads: WorkloadResult[]
  overheads: {
    workerBoot: OverheadStat
    noopRoundTrip: OverheadStat
    payloadRoundTrip: OverheadStat & { payloadBytes: number }
  }
  failures: string[]
}

export function median(samples: readonly number[]): number {
  if (samples.length === 0) {
    throw new Error('median of an empty sample set')
  }
  const sorted = [...samples].sort((a, b) => a - b)
  const mid = Math.floor(sorted.length / 2)
  const upper = sorted[mid] ?? 0
  if (sorted.length % 2 === 1) return upper
  return ((sorted[mid - 1] ?? 0) + upper) / 2
}

export function fmtMs(ms: number): string {
  if (ms >= 1000) return `${(ms / 1000).toFixed(2)} s`
  if (ms >= 100) return `${Math.round(ms)} ms`
  return `${ms.toFixed(1)} ms`
}
