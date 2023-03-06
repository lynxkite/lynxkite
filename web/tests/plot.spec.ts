// Tests the 'Custom plot' box.
import { test } from '@playwright/test';
import { Workspace } from './lynxkite';
import { resolve } from 'path';

let workspace: Workspace;
test.beforeAll(async ({ browser }) => {
  workspace = await Workspace.empty(await browser.newPage());
  await workspace.addBox({
    id: 'ib0',
    name: 'Import CSV',
    x: 100, y: 100,
  });
  const editor = await workspace.openBoxEditor('ib0');
  await editor.openGroup('Advanced settings');
  await editor.populateOperation({
    filename: resolve('tests/data/plot_data.csv'),
    columns: 'product,cnt',
  });

  await editor.loadImportedTable();
  await editor.close();
  /* global $ */
  await workspace.main.evaluate(e => { $(e).injector().get('environment').vegaConfig.renderer = 'svg'; });
});

test('a bar chart', async () => {
  const plotCode = {
    title: 'Simple bar chart',
    mark: 'bar',
    encoding: {
      x: { field: 'product', type: 'ordinal' },
      y: { field: 'cnt', type: 'quantitative' },
    }
  };
  await workspace.addBox({
    id: 'plot1',
    name: 'Custom plot',
    x: 200, y: 200, after: 'ib0',
    params: { plot_code: JSON.stringify(plotCode) },
  });
  const state = await workspace.openStateView('plot1', 'plot');
  await state.plot.expectBarHeightsToBe([56, 110, 86, 182]);
});
