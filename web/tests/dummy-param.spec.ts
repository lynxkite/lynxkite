// Tests "dummy" parameters, which are just labels in the parameter list.
import { test, expect } from '@playwright/test';
import { Workspace } from './lynxkite';

let workspace: Workspace;
test.beforeAll(async ({ browser }) => {
    workspace = await Workspace.empty(await browser.newPage());
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
    const editor = await workspace.openBoxEditor('rename-vertex-attrs');
    await expect(editor.element.locator('#text-title')).toHaveText('The new names for each attribute:');
    await expect(editor.element.locator('#text-title #help-button')).toBeVisible();
    await editor.close();
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
    const editor = await workspace.openBoxEditor('rename-edge-attrs');
    await expect(editor.element.locator('#text-title')).toHaveText('The new names for each attribute:');
    await expect(editor.element.locator('#text-title #help-button')).toBeVisible();
});
