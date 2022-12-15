//Tests copy-paste of the boxes
import { test, expect } from '@playwright/test';
import { Workspace } from './lynxkite';

let workspace: Workspace;

test.beforeAll(async ({ browser }) => {
    workspace = await Workspace.empty(browser);
    await workspace.addBox({ id: 'ex0', name: 'Create example graph', x: 100, y: 100 });
    await workspace.addBox({
        id: 'pr1',
        name: 'Compute PageRank',
        x: 100,
        y: 200,
        after: 'ex0',
        params: { name: 'page_rank_default', iterations: '1' },
    });
});

test('copy paste boxes', async function () {
    await workspace.selectBoxes(['pr1', 'ex0']);
    await workspace.page.keyboard.press(`Control+KeyC`);
    await workspace.page.keyboard.press(`Control+KeyV`);
    const ex0Editor = await workspace.openBoxEditor('create-example-graph_1');
    await ex0Editor.close();
    const pr1Editor = await workspace.openBoxEditor('compute-pagerank_1');
    await expect(pr1Editor.getParameter('direction', 'select')).toHaveValue('string:outgoing edges');
});
