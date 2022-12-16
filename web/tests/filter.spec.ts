// Tests filtering with the "Filter by attributes" box.
import { test, expect } from '@playwright/test';
import { Workspace } from './lynxkite';

let workspace: Workspace;
test.beforeAll(async ({ browser }) => {
  workspace = await Workspace.empty(browser);
  await workspace.addBox({ id: 'ex0', name: 'Create example graph', x: 100, y: 100 });
  await workspace.addBox({
    id: 'filter0',
    name: 'Filter by attributes',
    x: 100, y: 400, after: 'ex0', params: {
      'filterva_age': '<40',
      'filterva_name': 'Adam,Eve,Bob',
      'filterea_weight': '!1',
    }
  });
});

test('histograms after hard filters', async () => {
  const state = await workspace.openStateView('filter0', 'graph');
  await expect(state.left.vertexCount).toHaveText('2');
  await expect(state.left.edgeCount).toHaveText('1');
  expect(await state.left.vertexAttribute('name').getHistogramValues()).toEqual([
    { title: 'Adam', size: 100, value: 1 },
    { title: 'Eve', size: 100, value: 1 },
  ]);
  expect(await state.left.edgeAttribute('weight').getHistogramValues()).toEqual([
    { title: '2.00-2.00', size: 100, value: 1 },
  ]);
});
