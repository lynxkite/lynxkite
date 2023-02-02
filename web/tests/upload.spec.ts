// Test uploading a file in an import box.
import { test } from '@playwright/test';
import { Workspace } from './lynxkite';
import { resolve } from 'path';

test('can upload and import a simple CSV', async ({ page }) => {
  const workspace = await Workspace.empty(page);
  await workspace.addBox({ id: 'imp', name: 'Import CSV', x: 100, y: 100 });
  const box = await workspace.openBoxEditor('imp');
  const fileChooserPromise = page.waitForEvent('filechooser');
  const uploadButton = box.getParameter('filename', '[drop-tooltip="Upload"]');
  await uploadButton.click();
  const fileChooser = await fileChooserPromise;
  await fileChooser.setFiles(resolve('tests/data/upload_test.csv'));
  await box.loadImportedTable();
  const state = await workspace.openStateView('imp', 'table');
  await state.table.expect(
    ['name'],
    ['String'],
    [
      ['Adam'],
      ['Eve'],
      ['Bob'],
      ['Isolated Joe'],
    ]);
});
