// Tests box deletion
import { test, expect } from '@playwright/test';
import { Workspace } from './lynxkite';

let workspace: Workspace;
test.beforeAll(async ({ browser }) => {
  workspace = await Workspace.empty(await browser.newPage());
  await workspace.addBox({ id: 'ex0', name: 'Create example graph', x: 100, y: 100 });
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
});

test('delete boxes', async () => {
  await workspace.openBoxEditor('pr1');
  await workspace.openBoxEditor('pr2');// To check that only popups for deleted boxes are closed.
  await workspace.deleteBoxes(['pr1', 'ex0']);
  await expect(workspace.getBox('ex0')).not.toBeVisible();
  await expect(workspace.getBox('pr1')).not.toBeVisible();
  await expect(workspace.getBox('pr2')).toBeVisible();
  await expect(workspace.getBoxEditor('ex0').element).not.toBeVisible();
  await expect(workspace.getBoxEditor('pr1').element).not.toBeVisible();
  await expect(workspace.getBoxEditor('pr2').element).toBeVisible();
});
