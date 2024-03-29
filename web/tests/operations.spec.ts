// Tests for the operation UI.
import { test, expect } from '@playwright/test';
import { Workspace } from './lynxkite';

let workspace: Workspace;
test.beforeAll(async ({ browser }) => {
  workspace = await Workspace.empty(await browser.newPage());
  await workspace.addBox({ id: 'eg', name: 'Create example graph', x: 100, y: 100 });
});

test('attributes are alphabetically ordered', async () => {
  const sortedAttributes = ['age', 'gender', 'id', 'income', 'location', 'name'];

  await workspace.addBox({ id: 'str', name: 'Convert vertex attribute to String', x: 300, y: 200, after: 'eg' });
  let box = await workspace.openBoxEditor('str');
  await expect(box.operationParameter('attr').locator('option')).toHaveText(sortedAttributes);
  await box.close();

  await workspace.addBox({ id: 'agg', name: 'Aggregate vertex attribute globally', x: 300, y: 300, after: 'eg' });
  box = await workspace.openBoxEditor('agg');
  await expect(box.element.locator('operation-parameters .form-group label[for^=aggregate_]'))
    .toHaveText(sortedAttributes);
  await box.close();

  await workspace.addBox({ id: 'fil', name: 'Filter by attributes', x: 300, y: 400, after: 'eg' });
  box = await workspace.openBoxEditor('fil');
  await expect(box.element.locator('operation-parameters .form-group label[for^=filterva_]'))
    .toHaveText(sortedAttributes);
  await box.close();
});

test('popup closing order with escape', async () => {
  const popups = [
    await workspace.openBoxEditor('eg'),
    await workspace.openStateView('eg', 'graph'),
    await workspace.openBoxEditor('str'),
    await workspace.openStateView('str', 'graph'),
  ];
  for (const p of popups) {
    await expect(p.popup).toBeVisible();
  }
  for (let i = 0; i < popups.length; ++i) {
    await workspace.page.keyboard.press('Escape');
    // For each press of ESC we see one more popup gone, starting from the newest.
    for (let j = 0; j < popups.length; ++j) {
      if (popups.length - i - 1 <= j) {
        await expect(popups[j].popup).not.toBeVisible();
      } else {
        await expect(popups[j].popup).toBeVisible();
      }
    }
  }
});
