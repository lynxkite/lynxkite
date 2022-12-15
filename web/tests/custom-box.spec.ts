//
import { test, expect } from '@playwright/test';
import { Workspace } from './lynxkite';

let workspace: Workspace;

test('create custom box', async function ({ browser }) {
    workspace = await Workspace.empty(browser);
    await workspace.addWorkspaceParameter('prname', 'text', 'default_pr');
    await workspace.addBox({ id: 'in', name: 'Input', x: 100, y: 0, params: { name: 'in' } });
    await workspace.addBox({ id: 'eg', name: 'Create example graph', x: 400, y: 0 });
    await workspace.addBox({ id: 'pr', name: 'Compute PageRank', x: 100, y: 100, after: 'eg' });
    const editor = await workspace.openBoxEditor('pr');
    const params = {};
    params['name'] = '$prname';
    await editor.populateOperation(params);
    await editor.parametricSwitch('name').click();
    await editor.close();
    await workspace.addBox({ id: 'cc', name: 'Compute clustering coefficient', x: 100, y: 200, after: 'pr' });
    await workspace.addBox({ id: 'out', name: 'Output', x: 100, y: 300 });
    await workspace.connectBoxes('cc', 'graph', 'out', 'output');
    await workspace.editBox('out', { name: 'out' });
    await workspace.connectBoxes('in', 'input', 'pr', 'graph');
    await workspace.close();
});
