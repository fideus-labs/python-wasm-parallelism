import { defineConfig } from 'vitest/config'

// pool: 'forks' (child processes) instead of the default worker-thread pool:
// web-worker's Node shim checks worker_threads.isMainThread at import time and
// would treat vitest's own worker threads as a worker-side scope. Pyodide also
// boots real interpreters (~seconds each), hence the generous timeouts.
export default defineConfig({
  test: {
    pool: 'forks',
    include: ['tests/**/*.test.ts'],
    testTimeout: 120_000,
    hookTimeout: 120_000,
  },
})
