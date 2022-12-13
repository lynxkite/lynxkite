import { test, expect } from '@playwright/test';
import { Workspace } from './lynxkite';

let workspace: Workspace;
test.beforeAll(async ({ browser }) => {
    workspace = await Workspace.empty(browser);
    await workspace.addBox({ id: 'eg0', name: 'Create example graph', x: 100, y: 100 });
});

test('rename vertex attributes', async () => {
    await workspace.addBox({
        id: 'rename-vertex-attrs',
        name: 'Rename vertex attributes',
        x: 100,
        y: 200,
        after: 'eg0',
        params: { change_age: 'new_age' }
    });
    await workspace.clickBox('rename-vertex-attrs');
    await expect(workspace.page.locator('#text-title')).toHaveText('The new names for each attribute:');
    await expect(workspace.page.locator('#text-title #help-button')).toBeVisible();
    await workspace.clickBox('rename-vertex-attrs');
});

test('rename edge attributes', async () => {
    await workspace.addBox({
        id: 'rename-edge-attrs',
        name: 'Rename edge attributes',
        x: 200,
        y: 200,
        after: 'eg0',
        params: { change_weight: 'new_weight' }
    });
    await workspace.clickBox('rename-edge-attrs');
    await expect(workspace.page.locator('#text-title')).toHaveText('The new names for each attribute:');
    await expect(workspace.page.locator('#text-title #help-button')).toBeVisible();
});
