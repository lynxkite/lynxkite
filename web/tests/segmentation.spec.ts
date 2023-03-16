// Tests for segmentations of a graph.
import { test, expect } from '@playwright/test';
import { Workspace } from './lynxkite';

let workspace: Workspace;
test.beforeAll(async ({ browser }) => {
  workspace = await Workspace.empty(await browser.newPage());
  await workspace.addBox({ id: 'eg', name: 'Create example graph', x: 100, y: 100 });
});

test('segment by income', async () => {
  await workspace.addBox({
    id: 'segment-op', name: 'Segment by numeric attribute', x: 100, y: 200,
    after: 'eg', params: { attr: 'income', interval_size: '10' }
  });
  const state = await workspace.openStateView('segment-op', 'graph');
  await state.left.openSegmentation('bucketing');
  await expect(state.right.segmentCount).toHaveText('2');
  await state.close();
});

test('sub-segmentation can be opened', async () => {
  await workspace.addBox({
    id: 'sub-seg', name: 'Use base graph as segmentation', x: 100, y: 300,
    after: 'segment-op', params: {
      apply_to_graph: '.bucketing',
      name: 'copy'
    }
  });
  const state = await workspace.openStateView('sub-seg', 'graph');
  await state.left.openSegmentation('bucketing');
  await state.right.openSegmentation('copy');

  await expect(state.left.segmentCount).toHaveText('2');
  await expect(state.right.segmentCount).toHaveText('2');
  await expect(state.left.projectName).toHaveText('Base » bucketing');
  await expect(state.right.projectName).toHaveText('Base » bucketing » copy');
  // Close sub-segmentation on the right-hand side:
  await state.right.close();
  // This should reopen its grandparent on the left:
  await expect(state.left.projectName).toHaveText('Base');
  await expect(state.right.projectName).toHaveText('Base » bucketing');
  await state.close();
});

test('discard segmentation', async () => {
  { // Before.
    const state = await workspace.openStateView('segment-op', 'graph');
    await expect(state.left.segmentation('bucketing').element).toBeVisible();
    await state.close();
  }
  await workspace.addBox({
    id: 'discard-segment', name: 'Discard segmentation', x: 100, y: 400,
    after: 'sub-seg', params: { name: 'bucketing' }
  });
  { // After.
    const state = await workspace.openStateView('discard-segment', 'graph');
    await expect(state.left.segmentation('bucketing').element).not.toBeVisible();
    await state.close();
  }
});

test('segmentation size reporting - no empty segments', async () => {
  await workspace.addBox({
    id: 'copy', name: 'Use base graph as segmentation', x: 300, y: 200,
    after: 'eg', params: { name: 'self' }
  });
  const state = await workspace.openStateView('copy', 'graph');
  await state.left.openSegmentation('self');
  await expect(state.right.graphAttribute('segment-count').value).toHaveText('4');
  await expect(state.right.graphAttribute('total-segment-size').value).toHaveText('4');
  await expect(state.right.graphAttribute('total-segment-coverage').value).toHaveText('4');
  await state.close();
});

test('segmentation size reporting - has empty segments', async () => {
  await workspace.addBox({
    id: 'filter-op', name: 'Filter by attributes', x: 300, y: 300,
    after: 'copy', params: { 'filterva_income': '*' }
  });
  const state = await workspace.openStateView('filter-op', 'graph');
  await state.left.openSegmentation('self');
  await expect(state.right.graphAttribute('segment-count').value).toHaveText('4');
  await expect(state.right.graphAttribute('total-segment-size').value).toHaveText('2');
  await expect(state.right.graphAttribute('total-segment-coverage').value).toHaveText('2');
  await expect(state.right.graphAttribute('non-empty-segment-count').value).toHaveText('2');
  await state.close();
});
