// Test custom boxes and parametric parameters.
import { test, expect } from '@playwright/test';
import { Workspace, Splash, ROOT } from './lynxkite';

let splash: Splash;
let workspace: Workspace;
test.beforeAll(async ({ browser }) => {
  splash = await Splash.open(await browser.newPage());
});

async function setParametric(boxId: string, param: string, value: string) {
  const editor = await workspace.openBoxEditor(boxId);
  await editor.populateOperation({ [param]: value });
  await editor.parametricSwitch(param).click();
  await editor.close();
}

test.only('create custom box', async function () {
  await splash.newDirectory('custom_boxes');
  workspace = await splash.openNewWorkspace('test-custom-box');
  await workspace.editBox('anchor', { parameters: [{ id: 'prname', kind: 'text', defaultValue: 'default_pr' }] });
  await workspace.addBox({ id: 'in', name: 'Input', x: 200, y: 0, params: { name: 'in' } });
  // Temporary input for configuration.
  await workspace.addBox({ id: 'eg', name: 'Create example graph', x: 0, y: 110 });
  await workspace.addBox({ id: 'pr', name: 'Compute PageRank', x: 200, y: 100, after: 'eg' });
  await setParametric('pr', 'name', '$prname');
  await workspace.addBox({ id: 'cc', name: 'Compute clustering coefficient', x: 200, y: 200, after: 'pr' });
  await workspace.addBox({ id: 'out', name: 'Output', x: 200, y: 300, after: 'cc', params: { name: 'out' } });
  // Time to switch to the real input.
  await workspace.connectBoxes('in', 'input', 'pr', 'graph');
  await workspace.close();
});

test('use custom box', async () => {
  workspace = await splash.openNewWorkspace('test-example');
  await workspace.addBox({ id: 'eg', name: 'Create example graph', x: 100, y: 100 });
  await workspace.addBox({ id: 'cb', name: ROOT + '/custom_boxes/test-custom-box', x: 100, y: 200, after: 'eg' });
  const state = await workspace.openStateView('cb', 'out');
  await expect(state.left.vertexCount).toHaveText('4');
  await expect(state.left.edgeCount).toHaveText('4');
  await expect(state.left.vertexAttribute('default_pr').element).toBeVisible();
  await expect(state.left.vertexAttribute('clustering_coefficient').element).toBeVisible();
  await state.close();
});

test('use custom box with a parameter', async () => {
  await workspace.editBox('cb', { prname: 'custom_pr' });
  const state = await workspace.openStateView('cb', 'out');
  await expect(state.left.vertexAttribute('default_pr').element).not.toBeVisible();
  await expect(state.left.vertexAttribute('custom_pr').element).toBeVisible();
  await state.close();
});

test('dive into custom box', async () => {
  await workspace.selectBox('cb');
  await workspace.main.locator('#dive-down').click();
  let state = await workspace.openStateView('cc', 'graph');
  await expect(state.left.vertexAttribute('custom_pr').element).toBeVisible();
  await expect(state.left.vertexAttribute('clustering_coefficient').element).toBeVisible();
  await state.close();

  // Make a change.
  await workspace.editBox('cc', { name: 'clustco' });
  state = await workspace.openStateView('cc', 'graph');
  await expect(state.left.vertexAttribute('clustering_coefficient').element).not.toBeVisible();
  await expect(state.left.vertexAttribute('clustco').element).toBeVisible();
  await state.close();

  // Affects the higher level too.
  await workspace.main.locator('#dive-up').click();
  state = await workspace.openStateView('cb', 'out');
  await expect(state.left.vertexAttribute('clustering_coefficient').element).not.toBeVisible();
  await expect(state.left.vertexAttribute('clustco').element).toBeVisible();
  await state.close();
});

test('save as custom box with parameters', async () => {
  // Set up a few connected boxes.
  workspace = await splash.openNewWorkspace('test-for-custom-box');
  await workspace.editBox('anchor', { parameters: [{ id: 'prname', kind: 'text', defaultValue: 'default_pr' }] });
  await workspace.addBox({ id: 'eg', name: 'Create example graph', x: 100, y: 100 });
  await workspace.addBox({ id: 'pr1', name: 'Compute PageRank', x: 100, y: 200, after: 'eg' });
  await setParametric('pr1', 'name', '${prname}_1');
  await workspace.addBox({ id: 'pr2', name: 'Compute PageRank', x: 100, y: 300, after: 'pr1' });
  await setParametric('pr2', 'name', '${prname}_2');
  await workspace.addBox({ id: 'pr3', name: 'Compute PageRank', x: 100, y: 400, after: 'pr2' });
  await setParametric('pr3', 'name', '${prname}_3');

  async function checkOutput() {
    const state = await workspace.openStateView('pr3', 'graph');
    await expect(state.left.vertexAttribute('default_pr_1').element).toBeVisible();
    await expect(state.left.vertexAttribute('default_pr_2').element).toBeVisible();
    await expect(state.left.vertexAttribute('default_pr_3').element).toBeVisible();
    await state.close();
  }
  await checkOutput();

  // Now save "pr1" and "pr2" as a custom box.
  await workspace.selectBoxes(['pr1', 'pr2']);
  await workspace.main.locator('#save-selection-as-custom-box').click();
  await workspace.submitInlineInput(
    '#save-selection-as-custom-box-input', ROOT + '/custom_boxes/custom-box-from-selection');

  // Check that the box has been replaced.
  await expect(workspace.getBox('pr1')).not.toBeVisible();
  await expect(workspace.getBox('pr2')).not.toBeVisible();
  await expect(workspace.getBox('eg')).toBeVisible();
  await expect(workspace.getBox('pr3')).toBeVisible();

  // The output is still the same.
  await checkOutput();
});

test('save as custom box with mixed outputs', async () => {
  await splash.openNewWorkspace('custom_boxes/test-custom-box');
  await workspace.addBox({ id: 'eg', name: 'Create example graph', x: 0, y: 200 });
  await workspace.addBox({ id: 'pr1', name: 'Compute PageRank', x: 200, y: 100, after: 'eg' });
  await workspace.addBox({ id: 'pr2', name: 'Compute PageRank', x: 200, y: 200, after: 'eg' });
  await workspace.addBox({ id: 'pr3', name: 'Compute PageRank', x: 400, y: 200, after: 'pr2' });
  await workspace.selectBoxes(['eg', 'pr2']);
  await workspace.main.locator('#save-selection-as-custom-box').click();
  await workspace.submitInlineInput(
    '#save-selection-as-custom-box-input', ROOT + '/custom_boxes/custom-box-from-selection-mixed');
  await workspace.expectConnected('my-custom-box_1', 'graph', 'pr1', 'graph');
  await workspace.expectConnected('my-custom-box_1', 'graph-2', 'pr3', 'graph');
  await workspace.close();
});

test('browse-custom-box', async () => {
  await splash.openNewWorkspace('browse-custom-box-ws');
  await workspace.addBox({ id: 'eg1', name: 'Create example graph', x: 0, y: 200 });
  await workspace.selectBoxes(['eg1']);
  await workspace.main.locator('#save-selection-as-custom-box').click();
  await workspace.submitInlineInput(
    '#save-selection-as-custom-box-input', ROOT + '/custom_boxes/my-custom-box-to-browse-1');

  await workspace.addBox({ id: 'eg2', name: 'Create example graph', x: 0, y: 400 });
  await workspace.selectBoxes(['eg2']);
  await workspace.main.locator('#save-selection-as-custom-box').click();
  await workspace.submitInlineInput(
    '#save-selection-as-custom-box-input', ROOT + '/browse-custom-box-dir/custom_boxes/my-custom-box-to-browse-2');

  // Check top level elements.
  await workspace.main.locator('div[drop-tooltip="Custom boxes"]').click();
  const root = workspace.main.locator('operation-tree operation-tree-node[id="root"]');
  const dir = root.locator('#custom_boxes');
  await expect(dir).toBeVisible();
  await expect(root.locator('#my-custom-box-to-browse-1')).not.toBeVisible();

  // Test that the custom box in the dir is only present after click.
  await expect(dir.locator('#my-custom-box-to-browse-2')).not.toBeVisible();
  await dir.click();
  await expect(dir.locator('#my-custom-box-to-browse-1')).toBeVisible();
  await expect(dir.locator('#my-custom-box-to-browse-2')).not.toBeVisible();
});
