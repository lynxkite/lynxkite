// Tests for snapshots.
import { test, expect } from '@playwright/test';
import { Splash, ROOT } from './lynxkite';

let splash: Splash;
test.beforeAll(async ({ browser }) => {
  splash = await Splash.open(await browser.newPage());
});

const snapshotName = 'This is a snapshot!';

test('create a snapshot', async () => {
  const workspace = await splash.openNewWorkspace('test-create-snapshot');
  await workspace.addBox({ id: 'eg', name: 'Create example graph', x: 100, y: 100 });
  const state = await workspace.openStateView('eg', 'graph');
  const snapshotBox = state.popup.locator('#save-as-snapshot-box input');
  const snapshotButton = state.popup.locator('#save-as-snapshot-button');
  await snapshotButton.click();
  await snapshotBox.fill(ROOT + '/' + snapshotName);
  await snapshotBox.press('Enter');
  await workspace.close();
  await expect(splash.snapshot(snapshotName)).toBeVisible();
});

test('load snapshot in a new workspace', async () => {
  const workspace = await splash.openNewWorkspace('test-load-snapshot');
  await workspace.addBox({
    id: 'load', name: 'Import snapshot', params: { path: ROOT + '/' + snapshotName }, x: 100, y: 100,
  });
  const state = await workspace.openStateView('load', 'state');
  await expect(state.left.vertexCount).toHaveText('4');
  await expect(state.left.edgeCount).toHaveText('4');
  await expect(state.left.vertexAttributes).toHaveCount(6);
  await expect(state.left.edgeAttributes).toHaveCount(2);
  await expect(state.left.graphAttributes).toHaveCount(1);
  await workspace.close();
});

test('view snapshot in the directory browser', async () => {
  // Open the snapshot.
  await splash.snapshot(snapshotName).click();
  const state = await splash.snapshotState(ROOT + '/' + snapshotName);
  await expect(state.popup).toBeVisible();
  await expect(state.left.vertexCount).toHaveText('4');
  await expect(state.left.edgeCount).toHaveText('4');
  await expect(state.left.vertexAttributes).toHaveCount(6);
  await expect(state.left.edgeAttributes).toHaveCount(2);
  await expect(state.left.graphAttributes).toHaveCount(1);
  // Close the snapshot.
  await splash.snapshot(snapshotName).locator('.entry-header').click();
  await expect(state.popup).not.toBeVisible();
});
