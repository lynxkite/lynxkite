// Tests operation parameter default values.
import { test, expect } from '@playwright/test';
import { Workspace } from './lynxkite';

let workspace: Workspace;
test.beforeAll(async ({ browser }) => {
  workspace = await Workspace.empty(browser);
  await workspace.addBox({ id: 'ex0', name: 'Create example graph', x: 100, y: 100 });
});

test('test pagerank default choice values', async () => {
  await workspace.addBox({
    id: 'pr1',
    name: 'Compute PageRank',
    x: 100,
    y: 200,
    after: 'ex0',
    params: { name: 'page_rank_default', iterations: '1' },
  });
  await workspace.addBox({
    id: 'pr2',
    name: 'Compute PageRank',
    x: 100,
    y: 300,
    after: 'pr1',
    params: { name: 'page_rank_incoming', direction: 'incoming edges', iterations: '1' },
  });
  const state = await workspace.openStateView('pr2', 'graph');
  const defaultHistogram = await state.left.vertexAttribute('page_rank_default').getHistogramValues();
  const incomingHistogram = await state.left.vertexAttribute('page_rank_incoming').getHistogramValues();
  expect(defaultHistogram).not.toEqual(incomingHistogram);
  state.close();
});
