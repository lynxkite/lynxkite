// Tests operation parameter default values.
import { test, expect } from '@playwright/test';
import { Workspace } from './lynxkite';

let workspace: Workspace;
test.beforeAll(async ({ browser }) => {
  workspace = await Workspace.empty(browser);
  await workspace.addBox({ id: 'ex0', name: 'Create example graph', x: 100, y: 100 });
});

test('pagerank default choice values', async () => {
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

test('editing pagerank default choice values', async () => {
  await workspace.editBox('pr1', { direction: 'all edges' }); // change direction
  await workspace.editBox('pr2', { direction: 'all edges' }); // change direction
  let boxEditor = await workspace.openBoxEditor('pr1');
  await expect(boxEditor.getParameter('direction', 'select')).toHaveValue('string:all edges');
  await boxEditor.close();
  boxEditor = await workspace.openBoxEditor('pr2');
  await expect(boxEditor.getParameter('direction', 'select')).toHaveValue('string:all edges');
  await boxEditor.close();
});

test('multi-choice default values', async () => {
  await workspace.addBox({ id: 'ex', name: 'Create example graph', x: 100, y: 100 });
  await workspace.addBox({
    id: 'discard', name: 'Discard vertex attributes', x: 100, y: 200, after: 'ex'
  });
  const box = await workspace.openBoxEditor('discard')
  await box.moveTo(500, 100);
  const state = await workspace.openStateView('discard', 'graph');
  await state.moveTo(500, 400);
  const attrs = state.left.side.locator('entity[kind="vertex-attribute"]');
  await expect(attrs).toHaveText(['age', 'gender', 'id', 'income', 'location', 'name'], { useInnerText: true });
  await box.populateOperation({ name: ['income'] });
  await expect(attrs).toHaveText(['age', 'gender', 'id', 'location', 'name'], { useInnerText: true });
  await box.populateOperation({ name: ['income', 'location'] });
  await expect(attrs).toHaveText(['age', 'gender', 'id', 'name'], { useInnerText: true });
  await box.populateOperation({ name: [] });
  await expect(attrs).toHaveText(['age', 'gender', 'id', 'income', 'location', 'name'], { useInnerText: true });
});
