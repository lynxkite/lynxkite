// Tests the basic operation of a LynxKite workspace.
import { test, expect } from '@playwright/test';
import { Workspace } from './lynxkite';

let workspace: Workspace;
test.beforeAll(async ({ browser }) => {
  workspace = await Workspace.empty(await browser.newPage());
  await workspace.addBox({ id: 'eg0', name: 'Create example graph', x: 100, y: 100 });
});

test('has the expected contents', async () => {
  const state = await workspace.openStateView('eg0', 'graph');
  await expect(state.left.vertexCount).toHaveText('4');
  await expect(state.left.edgeCount).toHaveText('4');
  await expect(state.left.vertexAttributes.locator('.title')).toHaveText(
    ['age', 'gender', 'id', 'income', 'location', 'name']);
  await expect(state.left.edgeAttributes.locator('.title')).toHaveText(['comment', 'weight']);
  await expect(state.left.graphAttributes.locator('.title')).toHaveText(['greeting']);
  await expect(state.left.graphAttributes.locator('value')).toHaveText(['Hello world! 😀']);
  await state.close();
});

test('workspace with reverse edges has the proper vertex count', async () => {
  await workspace.addBox({
    id: 'reversed-edges', name: 'Add reversed edges', x: 100, y: 200, after: 'eg0'
  });
  const state = await workspace.openStateView('reversed-edges', 'graph');
  await expect(state.left.vertexCount).toHaveText('4');
  await expect(state.left.edgeCount).toHaveText('8');
  await expect(state.left.attributes).toHaveCount(8);
  await state.close();
});

test('workspace with reverse edges has colored plugs', async () => {
  await workspace.addBox({ id: 'eg1', name: 'Create example graph', x: 350, y: 100 });
  const outPlugFirstEG = workspace.getOutputPlug('eg0');
  const outPlugReversedEdges = workspace.getOutputPlug('reversed-edges');
  const outPlugSecondEG = workspace.getOutputPlug('eg1');
  await expect(outPlugFirstEG).not.toHaveClass('plug-progress-unknown');
  await expect(outPlugReversedEdges).not.toHaveClass('plug-progress-unknown');
  await expect(outPlugSecondEG).not.toHaveClass('plug-progress-unknown');
});
