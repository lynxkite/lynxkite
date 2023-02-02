//
import { test, expect } from '@playwright/test';
import { TableBrowser, Workspace } from './lynxkite';

let workspace: Workspace;

test.beforeAll(async ({ browser }) => {
  workspace = await Workspace.empty(browser);
  await workspace.addBox({ id: 'eg0', name: 'Create example graph', x: 100, y: 100 });
});

test('visualize with instrument', async function ({ browser }) {
  const popup = await workspace.openStateView('eg0', 'graph');
  await popup.setInstrument(0, 'visualize', {});
  await popup.left.vertexAttribute('name').visualizeAs('label');
  await expect(workspace.page.locator(".graph-view")).not.toHaveClass(/loading/);
  const graph = await popup.visualization.graphData();
  expect(graph.vertices).toConcur([
    { label: 'Adam' },
    { label: 'Eve' },
    { label: 'Bob' },
  ]);
});