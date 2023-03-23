// Tests the bar on the right side that lets you add boxes.
import { test, expect } from '@playwright/test';
import { Workspace } from './lynxkite';

test('adding a box', async ({ page }) => {
  await Workspace.empty(page, 'operation-selector');
  await expect(page.locator('g.box')).toHaveCount(1);
  await page.keyboard.type('/examp');
  await page.keyboard.press('Enter');
  await expect(page.locator('g.box')).toHaveCount(2);
});
