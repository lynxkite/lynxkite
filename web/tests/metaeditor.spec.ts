// Tests editing the "meta" side of box parameters.
import { test, expect } from '@playwright/test';
import { Workspace } from './lynxkite';

let workspace: Workspace;
test.beforeAll(async ({ browser }) => {
  workspace = await Workspace.empty(await browser.newPage());
  await workspace.addBox({ id: 'eg0', name: 'Create example graph', x: 100, y: 100 });
});

test('metaeditor can change box kind and id', async () => {
  let box = await workspace.openBoxEditor('eg0');
  const toggle = () => box.popup.locator('#toggle-meta').click();
  const id = () => box.popup.locator('#id input');
  const operation = () => box.popup.locator('#operation-id input');

  // Check.
  await toggle();
  await expect(id()).toHaveValue('eg0');
  await expect(operation()).toHaveValue('Create example graph');

  // Change.
  await id().fill('new-id');
  await operation().fill('Import CSV');
  box = workspace.getBoxEditor('new-id');
  await toggle();
  await expect(box.head()).toHaveText('Import CSV');
  await box.openGroup('Advanced settings');
  await expect(box.getCodeParameter('delimiter')).toHaveText(',');

  // Restore.
  toggle();
  await expect(id()).toHaveValue('new-id');
  await expect(operation()).toHaveValue('Import CSV');
  await id().fill('eg0');
  await operation().fill('Create example graph');
  box = workspace.getBoxEditor('eg0');
  await toggle();
  await expect(box.head()).toHaveText('Create example graph');
  await box.close();
});
