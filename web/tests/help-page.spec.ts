//Tests the help page
import { test, expect, Page } from '@playwright/test';

async function checkExactlyOneCurrent(page: Page) {
    // There is only one highlighted item:
    await expect(page.locator('span.find-highlight-current')).toHaveCount(1);
}

async function checkCurrentHighlight(page: Page, expectedPos) {
    await checkExactlyOneCurrent(page);
    // Check correct item:
    const cssQuery = 'span.find-highlight.find-highlight-current.find-highlight-' + expectedPos;
    await expect(page.locator(cssQuery)).toBeVisible();
}

test('search box in help', async ({ page }) => {
    await page.goto('#/help');
    const field = page.locator('#find-in-page-text');
    const help = page.locator('#whole-help');
    expect(await help.evaluate(e => e.scrollTop)).toBe(0);
    await expect(field).toBeVisible();
    // Search for the phrase "user".
    await field.fill('user');
    // Expect more than 5 matches.
    await expect(page.locator('span.find-highlight').first()).toBeVisible();
    expect(await page.locator('span.find-highlight').count()).toBeGreaterThan(5);
    // The first one is selected:
    await checkCurrentHighlight(page, 0);
    // Move to next:
    await page.keyboard.press('Enter');
    await checkCurrentHighlight(page, 1);
    // Move to next:
    await page.locator('#find-in-page-next').click();
    await checkCurrentHighlight(page, 2);
    // Move to prev:
    await page.locator('#find-in-page-prev').click();
    await checkCurrentHighlight(page, 1);
    // Move to prev:
    await page.locator('#find-in-page-prev').click();
    await checkCurrentHighlight(page, 0);
    // Move to prev:
    await page.locator('#find-in-page-prev').click();
    await checkExactlyOneCurrent(page);
    // Move to next:
    await page.locator('#find-in-page-next').click();
    await checkCurrentHighlight(page, 0);
    // Text not found:
    await field.click();
    await field.fill('qwertyui');
    await expect(page.locator('span.find-highlight')).toHaveCount(0)
});

test('scroll position in help', async ({ page }) => {
    await page.goto('#/help#graph-visualization');
    const help = page.locator('#whole-help');
    expect(await help.evaluate(e => e.scrollTop)).toBeGreaterThan(0);
});
