//Tests workspace fork
import { test, expect } from '@playwright/test';
import { Splash, Workspace } from './lynxkite';

let splash: Splash;
let workspace: Workspace;

test('fork workspace', async function ({ browser }) {
    splash = await Splash.open(await browser.newPage());
    workspace = await splash.openNewWorkspace('test-example');
    await workspace.page.pause();
    await workspace.page.locator('#save-workspace-as-starter-button').click();
    await workspace.page.locator('#save-workspace-as-input').locator('input').fill('test-example-fork');
    await workspace.page.locator('#save-workspace-as-input').locator('#ok').click();
    await workspace.page.waitForURL('**/#/workspace/test-example-fork');
    await workspace.close();
    await splash.expectWorkspaceListed('test-example-fork');
});
