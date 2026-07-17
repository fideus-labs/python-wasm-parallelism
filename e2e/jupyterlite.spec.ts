/**
 * JupyterLite smoke tests (project `jupyterlite`, served by
 * scripts/serve-lite.mjs on 8000 with COOP/COEP — see playwright.config.ts).
 *
 * Each notebook is executed the way a user would — Run menu → Run All Cells
 * in the real JupyterLab UI — but the assertions read the notebook MODEL
 * (via `window.jupyterapp`), not the DOM: JupyterLab 4 virtualizes cell
 * rendering, so off-screen outputs simply aren't in the DOM and a DOM-only
 * sweep can miss both errors and the success marker. The model always holds
 * every cell's outputs.
 *
 * The success marker is each notebook's own last cell: it asserts the
 * parallel/soft-capped run beat its serial baseline and then prints
 * `<MARKER> <speedup>` — so the marker's presence already proves the
 * speedup claim held inside the kernel.
 */
import type { Page } from '@playwright/test'
import { ALLOWED_CONSOLE_ERRORS, expect, makeConsoleWatchdogTest } from './fixtures.js'

const test = makeConsoleWatchdogTest([
  ...ALLOWED_CONSOLE_ERRORS,
  // Known upstream race in jupyterlite-pyodide-kernel's coincident transport
  // (the SharedArrayBuffer RPC between the kernel worker and the main
  // thread): `Atomics.waitAsync` returns `{ value: 'not-equal' }` — a string,
  // not a promise — when the notify lands before the wait starts, and
  // coincident calls `.value.then(...)` on it unconditionally. The throw
  // drops one in-flight kernel message (observed effect: a single cell's
  // execution-count prompt stays empty in an otherwise complete run). The
  // demo outcome is unaffected; the model audit below still proves every
  // cell ran and none errored.
  /value\.then is not a function/,
])

/** Flattened view of one notebook cell as stored in the model. */
interface CellSnapshot {
  cellType: string
  executionCount: number | null
  /** One entry per output: `stream`/`execute_result` text, or `ename: evalue`. */
  outputs: { outputType: string; text: string }[]
}

/**
 * Snapshot every cell of the current notebook from the document model.
 * Returns null until the notebook widget and its context exist.
 */
async function readNotebookModel(page: Page): Promise<CellSnapshot[] | null> {
  return page.evaluate(() => {
    interface NbOutput {
      output_type: string
      text?: string | string[]
      ename?: string
      evalue?: string
      data?: Record<string, unknown>
    }
    interface NbCell {
      cell_type: string
      execution_count?: number | null
      outputs?: NbOutput[]
    }
    const app = (
      window as unknown as {
        jupyterapp?: {
          shell?: { currentWidget?: { context?: { model?: { toJSON(): unknown } } } }
        }
      }
    ).jupyterapp
    const model = app?.shell?.currentWidget?.context?.model
    if (!model) return null
    const notebook = model.toJSON() as { cells?: NbCell[] }
    if (!Array.isArray(notebook.cells)) return null
    return notebook.cells.map((cell) => ({
      cellType: cell.cell_type,
      executionCount: cell.execution_count ?? null,
      outputs: (cell.outputs ?? []).map((output) => {
        let text = ''
        if (typeof output.text === 'string') text = output.text
        else if (Array.isArray(output.text)) text = output.text.join('')
        else if (output.ename !== undefined) text = `${output.ename}: ${output.evalue ?? ''}`
        else if (output.data !== undefined) text = JSON.stringify(Object.keys(output.data))
        return { outputType: output.output_type, text }
      }),
    }))
  })
}

/**
 * Open `notebookPath`, Run All Cells through the real menu bar, wait for
 * `<markerName> <speedup>` in the last cell, then audit the full model:
 * no error outputs anywhere, speedup ≥ 1, marker visible in the UI.
 */
async function runNotebookToMarker(
  page: Page,
  consoleErrors: string[],
  notebookPath: string,
  markerName: string,
): Promise<void> {
  const marker = new RegExp(`${markerName} (\\d+(?:\\.\\d+)?)`)

  await page.goto(`/lab/index.html?path=${notebookPath}`)

  // The notebook opened in the Lab shell and its cells are loaded.
  await expect(page.locator('.jp-NotebookPanel .jp-Notebook').first()).toBeVisible()
  await expect(page.locator('.jp-Cell').first()).toBeVisible()

  // Wait for the Pyodide kernel to reach idle before driving the menu:
  // `notebook:run-all-cells` silently does nothing while the session has no
  // kernel yet, and the in-browser kernel takes seconds to boot.
  await expect
    .poll(
      () =>
        page.evaluate(() => {
          const app = (
            window as unknown as {
              jupyterapp?: {
                shell?: {
                  currentWidget?: {
                    sessionContext?: { session?: { kernel?: { status?: string } | null } | null }
                  }
                }
              }
            }
          ).jupyterapp
          return app?.shell?.currentWidget?.sessionContext?.session?.kernel?.status ?? 'unknown'
        }),
      { timeout: 120_000 },
    )
    .toBe('idle')

  // Run menu → Run All Cells, through the real menu bar.
  await page.getByRole('menuitem', { name: 'Run', exact: true }).click()
  await page.getByRole('menuitem', { name: 'Run All Cells', exact: true }).click()

  // Wait until the last cell's output carries the success marker. Everything
  // upstream (wheel installs, nested-worker pool boots, the timed runs,
  // matplotlib download) has to finish first, so this is the long wait.
  await expect
    .poll(
      async () => {
        const cells = await readNotebookModel(page)
        const last = cells?.at(-1)
        return last?.outputs.some((output) => marker.test(output.text)) ? 'marker printed' : 'running'
      },
      { timeout: 200_000, intervals: [2_000] },
    )
    .toBe('marker printed')

  // Full-model audit: no cell produced an error output, and the marker's
  // speedup is sane (its cell already asserted parallel < serial, so a
  // rounded value below 1.0 is impossible). Execution counts are deliberately
  // NOT asserted — the coincident race documented on the allow-list above can
  // eat a single execute_input, leaving one prompt empty in a complete run.
  // "Every cell ran" is already proven by the marker: the cells form a
  // dependency chain, so a skipped or failed cell surfaces as a NameError
  // downstream, never as a silent pass.
  const cells = await readNotebookModel(page)
  expect(cells).not.toBeNull()
  const codeCells = cells!.filter((cell) => cell.cellType === 'code')
  expect(codeCells.length).toBeGreaterThan(0)
  for (const cell of codeCells) {
    for (const output of cell.outputs) {
      expect(output.outputType, `error output: ${output.text}`).not.toBe('error')
    }
  }
  const markerText = codeCells
    .at(-1)!
    .outputs.map((output) => output.text)
    .join('')
  const speedup = Number(marker.exec(markerText)?.[1])
  expect(speedup).toBeGreaterThanOrEqual(1)

  // The marker is also visible in the rendered UI: scroll the virtualized
  // notebook to the bottom so the last cell materializes in the DOM.
  await expect(async () => {
    await page.evaluate(() => {
      const scroller =
        document.querySelector('.jp-WindowedPanel-outer') ?? document.querySelector('.jp-Notebook')
      if (scroller) scroller.scrollTop = scroller.scrollHeight
    })
    // Scoped to output areas: the cell's own source (`print("<MARKER>"…`)
    // also contains the string, and `\d` keeps the editor's tokenized
    // string-literal span from matching.
    await expect(
      page.locator('.jp-OutputArea-output', { hasText: new RegExp(`${markerName} \\d`) }),
    ).toBeVisible({ timeout: 2_000 })
  }).toPass({ timeout: 30_000 })

  // Nothing escaped to the console during the whole run (the auto fixture
  // re-checks at teardown; this pins it to the moment the run finished).
  expect(consoleErrors).toEqual([])
}

test('01-pool-basics.ipynb: Run All Cells prints POOL_DEMO_OK with no errors', async ({
  page,
  consoleErrors,
}) => {
  await runNotebookToMarker(page, consoleErrors, '01-pool-basics.ipynb', 'POOL_DEMO_OK')
})

test('04-multiprocessing.ipynb: Run All Cells prints MP_DEMO_OK with no errors', async ({
  page,
  consoleErrors,
}) => {
  // The marker cell asserts Pool(4) beat Pool(1) on the same dispatch path
  // and prints the ratio — so marker presence = the soft cap works and the
  // one-line-port, AsyncResult, and capability-detection cells all ran clean.
  await runNotebookToMarker(page, consoleErrors, '04-multiprocessing.ipynb', 'MP_DEMO_OK')
})
