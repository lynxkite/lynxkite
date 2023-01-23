// Test what happens if a box has saved parameters that the box no longer expects.
import { test, expect } from '@playwright/test';
import { Workspace } from './lynxkite';

test('test unexpected parameters', async ({ page }) => {
  const workspace = await Workspace.empty(page);
  await workspace.addBox({ id: 'ex', name: 'Create example graph', x: 100, y: 0 });
  await workspace.addBox({
    id: 'attr', name: 'Add constant vertex attribute',
    params: { name: 'x' },
    x: 100, y: 100, after: 'ex',
  });
  await workspace.addBox({
    id: 'agg', name: 'Aggregate on neighbors',
    params: { 'aggregate_x': ['average', 'count'] },
    x: 100, y: 200, after: 'attr',
  });
  const attr = await workspace.openBoxEditor('attr');
  const agg = await workspace.openBoxEditor('agg');
  const output = workspace.getOutputPlug('agg');
  // Original setup.
  await expect(agg.operationParameter('aggregate_x')).toBeVisible();
  await expect(agg.operationParameter('aggregate_y')).not.toBeVisible();
  await expect(output).not.toHaveClass(/plug-progress-error/);
  // Change attribute name.
  await attr.populateOperation({ name: 'y' });
  await expect(agg.operationParameter('aggregate_x')).toBeVisible();
  await expect(agg.operationParameter('aggregate_y')).toBeVisible();
  await expect(agg.getParameter('aggregate_x')).toHaveValue('average,count');
  await expect(output).toHaveClass(/plug-progress-error/);
  // Remove unexpected parameter.
  await agg.removeParameter('aggregate_x');
  await expect(agg.operationParameter('aggregate_x')).not.toBeVisible();
  await expect(agg.operationParameter('aggregate_y')).toBeVisible();
  await expect(output).not.toHaveClass(/plug-progress-error/);
});
