//Tests drag selection
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
  });
  await workspace.addBox({
    id: 'pr2',
    name: 'Compute PageRank',
    x: 100,
    y: 300,
    after: 'pr1',
  });
});
test('select boxes with mouse drag', async () => {
  await workspace.page.keyboard.down('Shift');
  await workspace.getBox('pr2').dragTo(workspace.getBox('pr2'), {
    force: true,
    sourcePosition: {
      x: -5,
      y: 100,
    },
    targetPosition: {
      x: 200,
      y: -200,
    }
  });
  await workspace.page.keyboard.up('Shift');
  await workspace.expectNumSelectedBoxes(4);
});
