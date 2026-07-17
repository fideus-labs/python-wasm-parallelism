/** Type surface of scripts/bundles.mjs for the TS consumers (tests/). */
import type { BuildOptions, BuildResult } from 'esbuild'

export declare const rootDir: string
export declare function buildWorkerBundle(options?: BuildOptions): Promise<BuildResult>
export declare function buildIndexBundle(options?: BuildOptions): Promise<BuildResult>
export declare function buildBrowserBundle(
  options?: BuildOptions,
): Promise<{ outfile: string; pyodideVersion: string }>
