import { defineConfig } from 'vitest/config'

// Pyodide boots real interpreters (~seconds each), hence the generous
// timeouts. fileParallelism: false — every test file esbuilds the same
// dist/pyodide-worker.js at startup (tests/helpers.ts buildWorkerBundle or a
// local beforeAll); concurrent files would race on the shared outfile.
export default defineConfig({
  test: {
    fileParallelism: false,
    include: ['tests/**/*.test.ts'],
    testTimeout: 120_000,
    hookTimeout: 120_000,
  },
})
