// Tests for the help content outside of the full help page.
import { test, expect } from '@playwright/test';
import { Workspace } from './lynxkite';

test('help popup in category browser', async ({page}) => {
  await Workspace.empty(page);
  await page.locator('.fa-upload').click();
  const category = page.locator('.box.green');
  // Help is not visible.
  await expect(page.getByText('CSV stands for comma-separated values.')).not.toBeVisible();
  // Help appears on hover.
  await category.getByText('Import CSV').locator('#help-button').hover();
  await expect(page.getByText('CSV stands for comma-separated values.')).toBeVisible();
  // Help disappears if we don't hover. (We hover over the text.)
  await category.getByText('Import CSV').hover();
  await expect(page.getByText('CSV stands for comma-separated values.')).not.toBeVisible();
});

test('help popups in box parameters', async ({page}) => {
  const ws = await Workspace.empty(page);
  await ws.addBox({ id: 'box', name: 'Create Mocnik random graph', x: 100, y: 100 });
  const editor = await ws.openBoxEditor('box');

  await expect(editor.popup.getByText('Mocnik et al')).not.toBeVisible();
  await editor.popup.getByText('More about').click();
  await expect(editor.popup.getByText('Mocnik et al')).toBeVisible();
  await editor.popup.getByText('Less about').click();
  await expect(editor.popup.getByText('Mocnik et al')).not.toBeVisible();

  const parameterTexts = ['Number of vertices', 'Dimension of space', 'Density of graph', 'Random seed'];
  await expect(page.locator('#help-popup')).not.toBeVisible();
  for (const p of parameterTexts) {
    const help = editor.popup.getByText(p).locator('#help-button');
    await help.click();
    await expect(page.locator('#help-popup')).toBeVisible();
    await help.click();
    await expect(page.locator('#help-popup')).not.toBeVisible();
  }
});
