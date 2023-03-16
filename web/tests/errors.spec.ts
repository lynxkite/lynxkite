// Tests error reporting.
import { test, expect } from '@playwright/test';
import { Workspace } from './lynxkite';

let workspace: Workspace;
test.beforeAll(async ({ browser }) => {
  workspace = await Workspace.empty(await browser.newPage(), 'errors');
  await workspace.addBox({ id: 'eg', name: 'Create example graph', x: 100, y: 100 });
});

test('unconnected inputs', async () => {
  await workspace.addBox({ id: 'sql', name: 'SQL1', x: 300, y: 150 });
  const editor = await workspace.openBoxEditor('sql');
  await expect(editor.element).toContainText('Cannot retrieve box metadata.');
  await expect(editor.element).toContainText('Input input of box sql is not connected.');
  const state = await workspace.openStateView('sql', 'table');
  await expect(state.popup).toContainText('Failed to generate output state.');
  await expect(state.popup).toContainText('Input input of box sql is not connected.');
  // But after connecting it it works.
  await workspace.connectBoxes('eg', 'graph', 'sql', 'input');
  await expect(editor.element).toContainText('Summary');
  await expect(state.popup).toContainText('Isolated Joe');
  await workspace.deleteBoxes(['sql']);
});

test('error in scalar', async () => {
  await workspace.addBox({
    id: 'x', name: 'Derive vertex attribute', x: 100, y: 200, after: 'eg', params: {
      output: 'empty', expr: 'Option.empty[Double]',
    }});
  await workspace.addBox({
    id: 'aggr', name: 'Aggregate vertex attribute globally', x: 100, y: 300, after: 'x', params: {
      aggregate_income: ['average'], aggregate_empty: ['average', 'sum'],
    }});
  const state = await workspace.openStateView('aggr', 'graph');
  await expect(state.left.graphAttributes.locator('.title')).toHaveText([
    'empty_average', 'empty_sum', 'greeting', 'income_average']);
  await expect(state.left.graphAttributes.locator('value')).toContainText([
    '↻', '0', 'Hello world! 😀', '2k']);
  // Check non-error behavior while we're here.
  await state.left.graphAttribute('income_average').element.locator('value').click();
  await expect(state.left.graphAttributes.locator('value')).toContainText([
    '↻', '0', 'Hello world! 😀', '1500']);
  // Check error.
  await state.left.graphAttribute('empty_average').element.locator('.value-error').click();
  const modal = workspace.page.locator('.modal-dialog');
  await expect(modal).toContainText('Average of empty set');
  await modal.locator('#close-modal-button').click();
  await expect(modal).not.toBeVisible();
});
