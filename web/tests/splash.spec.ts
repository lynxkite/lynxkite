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
    document.styleSheets[0].insertRule(
      '.spark-status, .user-menu { position: static !important; }');
  });
  page.splash = new lk.Splash(page);
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

test('empty test-example workspace',
  async function () {
    await page.splash.openNewWorkspace('test-example');
    await page.workspace.expectCurrentWorkspaceIs('test-example');
  });

test('test-example renamed and moved',
  async function () {
    // Go to project list.
    await page.workspace.close();
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

test('a few workspaces created',
  async function () {
    // We create this structure:
    // [plum]
    //   [orange]
    //     kiwi
    //   grape
    // apple
    // pear
    page.splash.openNewWorkspace('apple');
    page.workspace.close();
    page.splash.openNewWorkspace('pear');
    page.workspace.close();
    page.splash.newDirectory('plum');
    page.splash.openNewWorkspace('grape');
    page.workspace.close();
    page.splash.newDirectory('orange');
    page.splash.openNewWorkspace('kiwi');
    page.workspace.close();
    page.splash.popDirectory();
    page.splash.popDirectory();

    // expect
    page.splash.expectWorkspaceListed('apple');
    page.splash.expectWorkspaceListed('pear');
    page.splash.expectDirectoryListed('plum');
    page.splash.expectNumWorkspaces(2);
    page.splash.expectNumDirectories(1);

    page.splash.openDirectory('plum');
    page.splash.expectWorkspaceListed('grape');
    page.splash.expectDirectoryListed('orange');
    page.splash.expectNumWorkspaces(1);
    page.splash.expectNumDirectories(1);

    page.splash.openDirectory('orange');
    page.splash.expectWorkspaceListed('kiwi');
    page.splash.expectNumWorkspaces(1);
    page.splash.expectNumDirectories(0);

    page.splash.popDirectory();
    page.splash.popDirectory();
    page.splash.expectWorkspaceListed('apple');
  });

test(
  'search works as intended',
  async function () {
    page.splash.enterSearchQuery('a');
    page.splash.expectDirectoryListed('plum/orange');
    page.splash.expectWorkspaceListed('apple');
    page.splash.expectWorkspaceListed('pear');
    page.splash.expectWorkspaceListed('plum/grape');
    page.splash.expectNumWorkspaces(3);
    page.splash.expectNumDirectories(1);

    page.splash.enterSearchQuery('a g');
    page.splash.expectDirectoryListed('plum/orange');
    page.splash.expectWorkspaceListed('plum/grape');
    page.splash.expectNumWorkspaces(1);
    page.splash.expectNumDirectories(1);

    page.splash.enterSearchQuery('kiwi');
    page.splash.expectWorkspaceListed('plum/orange/kiwi');
    page.splash.expectNumWorkspaces(1);
    page.splash.expectNumDirectories(0);

    page.splash.enterSearchQuery('orange');
    page.splash.expectDirectoryListed('plum/orange');
    page.splash.expectNumWorkspaces(0);
    page.splash.expectNumDirectories(1);

    page.splash.clearSearchQuery();
    page.splash.openDirectory('plum');
    page.splash.enterSearchQuery('e');
    page.splash.expectDirectoryListed('orange');
    page.splash.expecWorkspaceListed('grape');
    page.splash.expectNumWorkspaces(1);
    page.splash.expectNumDirectories(1);
    page.splash.popDirectory();
  });

test('navigate dirs by clicking on path segment',
  async function () {
    page.splash.newDirectory('red');
    page.splash.newDirectory('green');
    page.splash.newDirectory('blue');
    page.splash.setDirectory(2);
    page.splash.expectDirectoryListed('blue');
    page.splash.setDirectory(1);
    page.splash.expectDirectoryListed('green');
    page.splash.popDirectory();
  });

test(
  'selected directory path does not contain spaces',
  async function () {
    page.splash.newDirectory('first');
    page.splash.newDirectory('second');
    page.splash.newDirectory('last');
    page.splash.expectSelectedCurrentDirectory('first/second/last');
    page.splash.popDirectory();
    page.splash.popDirectory();
    page.splash.popDirectory();
  });
