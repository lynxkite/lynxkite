//Tests workspace fork
import { test, expect } from '@playwright/test';
import { Splash, Workspace } from './lynxkite';

let splash: Splash;
let workspace: Workspace;

test('fork workspace', async function ({ browser }) {
    splash = await Splash.open(browser);
    workspace = await splash.openNewWorkspace('test-example');
    await workspace.page.locator('#save-workspace-as-starter-button').click();
    await workspace.page.locator('#save-workspace-as-input input').fill('automated-tests/subdirectory/test-example-fork');
    await workspace.page.locator('#save-workspace-as-input #ok').click();
    await workspace.page.waitForURL('**/#/workspace/automated-tests/subdirectory/test-example-fork');
    await workspace.close();
    await splash.expectWorkspaceListed('test-example-fork');
});
