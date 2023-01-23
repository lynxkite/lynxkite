//Tests the help page
import { test, expect } from '@playwright/test';
import { TableBrowser, Workspace } from './lynxkite';

let workspace: Workspace;

async function checkExactlyOneCurrent() {
    // There is only one highlighted item:
    await expect(await workspace.page.locator('span.find-highlight-current').count()).toBe(1);
}

async function checkCurrentHighlight(expectedPos) {
    await checkExactlyOneCurrent();
    // Check correct item:
    const cssQuery = 'span.find-highlight.find-highlight-current.find-highlight-' + expectedPos;
    await expect(workspace.page.locator(cssQuery)).toBeVisible();
}

test.beforeAll(async ({ browser }) => {
    workspace = await Workspace.empty(browser);
});

test('search box in help', async () => {
    await workspace.page.goto('#/help');
    const field = await workspace.page.locator('#find-in-page-text');
    await expect(field).toBeVisible;
    // Search for the phrase "user".
    await field.fill('user');
    // Expect more than 5 matches.
    await workspace.page.locator('span.find-highlight').first().waitFor();
    await expect(await workspace.page.locator('span.find-highlight').count()).toBeGreaterThan(5);
    // The first one is selected:
    await checkCurrentHighlight(0);
    // Move to next:
    await workspace.page.keyboard.press('Enter');
    await checkCurrentHighlight(1);
    // Move to next:
    await workspace.page.locator('#find-in-page-next').click();
    await checkCurrentHighlight(2);
    // Move to prev:
    await workspace.page.locator('#find-in-page-prev').click();
    await checkCurrentHighlight(1);
    // Move to prev:
    await workspace.page.locator('#find-in-page-prev').click();
    await checkCurrentHighlight(0);
    // Move to prev:
    await workspace.page.locator('#find-in-page-prev').click();
    await checkExactlyOneCurrent();
    // Move to next:
    await workspace.page.locator('#find-in-page-next').click();
    await checkCurrentHighlight(0);
    // Text not found:
    await field.click();
    await field.fill('qwertyui');
    await expect(await workspace.page.locator('span.find-highlight').count()).toBe(0);
});

test('scroll position in help', async () => {
    await workspace.page.goto('#/help#graph-visualization');
    const help = await workspace.page.locator('#whole-help');
    await expect(help).not.toHaveAttribute('scrollTop', '0');
});
