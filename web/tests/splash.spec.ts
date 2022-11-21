import { test, expect, Browser } from '@playwright/test';
import * as lk from './lynxkite.ts';

async function newSplash(browser: Browser) {
  const page = await browser.newPage();
  await page.goto('/#/');
  await page.evaluate(() => {
    window.sessionStorage.clear();
    window.localStorage.clear();
    window.localStorage.setItem('workspace-drawing-board tutorial done', 'true');
    window.localStorage.setItem('entry-selector tutorial done', 'true');
    window.localStorage.setItem('allow data collection', 'false');
    // Floating elements can overlap buttons and block clicks.
    document.styleSheets[0].insertRule('.spark-status, .user-menu { position: static !important; }');
  });
  await page.goto('/#/dir/');
  page.splash = new lk.Splash(page);
  await page.splash.expectDirectoryListed('built-ins'); // Make sure the page is loaded.
  if (await page.splash.directory('automated-tests').isVisible()) {
    await page.splash.deleteDirectory('automated-tests');
  }
  await page.splash.newDirectory('automated-tests');
  await page.splash.expectNumWorkspaces(0);
  await page.splash.expectNumDirectories(0);
  return page;
}

test.describe.configure({ mode: 'serial' });
let page: Page;
test.beforeAll(async ({ browser }) => {
  page = await newSplash(browser);
});
test.afterAll(async () => {
  await page.close();
});

test('empty test-example workspace', async function () {
  await page.splash.openNewWorkspace('test-example');
  await page.workspace.expectCurrentWorkspaceIs('test-example');
  await page.workspace.close();
});

test('test-example renamed and moved', async function () {
  await page.splash.expectWorkspaceListed('test-example');

  // Create directory.
  await page.splash.newDirectory('test-dir');
  await page.splash.expectWorkspaceNotListed('test-example');

  // Go back and move the project into the directory.
  await page.splash.popDirectory();
  await page.splash.expectWorkspaceListed('test-example');
  await page.splash.renameWorkspace('test-example', 'test-dir/test-example');
  await page.splash.expectWorkspaceNotListed('test-example');

  // Open directory.
  await page.splash.openDirectory('test-dir');
  await page.splash.expectWorkspaceListed('test-example');

  // Rename project.
  await page.splash.renameWorkspace('test-example', 'test-dir/test-renamed');
  await page.splash.expectWorkspaceListed('test-renamed');
  await page.splash.expectWorkspaceNotListed('test-example');

  // Delete project.
  await page.splash.deleteWorkspace('test-renamed');
  await page.splash.expectWorkspaceNotListed('test-renamed');

  // Go back and delete the directory.
  await page.splash.popDirectory();
  await page.splash.expectWorkspaceNotListed('test-renamed');
  await page.splash.expectDirectoryListed('test-dir');
  await page.splash.deleteDirectory('test-dir');
  await page.splash.expectDirectoryNotListed('test-dir');
});

test('a few workspaces created', async function () {
  // We create this structure:
  // [plum]
  //   [orange]
  //     kiwi
  //   grape
  // apple
  // pear
  await page.pause();
  await page.splash.openNewWorkspace('apple');
  await page.workspace.close();
  await page.splash.openNewWorkspace('pear');
  await page.workspace.close();
  await page.splash.newDirectory('plum');
  await page.splash.openNewWorkspace('grape');
  await page.workspace.close();
  await page.splash.newDirectory('orange');
  await page.splash.openNewWorkspace('kiwi');
  await page.workspace.close();
  await page.splash.popDirectory();
  await page.splash.popDirectory();

  // expect
  await page.splash.expectWorkspaceListed('apple');
  await page.splash.expectWorkspaceListed('pear');
  await page.splash.expectDirectoryListed('plum');
  await page.splash.expectNumWorkspaces(2);
  await page.splash.expectNumDirectories(1);

  await page.splash.openDirectory('plum');
  await page.splash.expectWorkspaceListed('grape');
  await page.splash.expectDirectoryListed('orange');
  await page.splash.expectNumWorkspaces(1);
  await page.splash.expectNumDirectories(1);

  await page.splash.openDirectory('orange');
  await page.splash.expectWorkspaceListed('kiwi');
  await page.splash.expectNumWorkspaces(1);
  await page.splash.expectNumDirectories(0);

  await page.splash.popDirectory();
  await page.splash.popDirectory();
  await page.splash.expectWorkspaceListed('apple');
});

test('search works as intended', async function () {
  await page.splash.enterSearchQuery('a');
  await page.splash.expectDirectoryListed('orange');
  await page.splash.expectWorkspaceListed('apple');
  await page.splash.expectWorkspaceListed('pear');
  await page.splash.expectWorkspaceListed('grape');
  await page.splash.expectNumWorkspaces(3);
  await page.splash.expectNumDirectories(1);

  await page.splash.enterSearchQuery('a g');
  await page.splash.expectDirectoryListed('orange');
  await page.splash.expectWorkspaceListed('grape');
  await page.splash.expectNumWorkspaces(1);
  await page.splash.expectNumDirectories(1);

  await page.splash.enterSearchQuery('kiwi');
  await page.splash.expectWorkspaceListed('kiwi');
  await page.splash.expectNumWorkspaces(1);
  await page.splash.expectNumDirectories(0);

  await page.splash.enterSearchQuery('orange');
  await page.splash.expectDirectoryListed('orange');
  await page.splash.expectNumWorkspaces(0);
  await page.splash.expectNumDirectories(1);

  await page.splash.clearSearchQuery();
  await page.splash.openDirectory('plum');
  await page.splash.enterSearchQuery('e');
  await page.splash.expectDirectoryListed('orange');
  await page.splash.expectWorkspaceListed('grape');
  await page.splash.expectNumWorkspaces(1);
  await page.splash.expectNumDirectories(1);
  await page.splash.popDirectory();
});

test('navigate dirs by clicking on path segment', async function () {
  await page.splash.newDirectory('red');
  await page.splash.newDirectory('green');
  await page.splash.newDirectory('blue');
  await page.splash.clickBreadcrumb('green');
  await page.splash.expectDirectoryListed('blue');
  await page.splash.clickBreadcrumb('red');
  await page.splash.expectDirectoryListed('green');
  await page.splash.popDirectory();
});

test('selected directory path does not contain spaces', async function () {
  await page.splash.newDirectory('first');
  await page.splash.newDirectory('second');
  await page.splash.newDirectory('last');
  await page.splash.expectSelectedCurrentDirectory('first/second/last');
  await page.splash.popDirectory();
  await page.splash.popDirectory();
  await page.splash.popDirectory();
});
