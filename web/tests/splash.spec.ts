// Tests the "splash" page where you can put workspaces in directories.
import { test, expect } from '@playwright/test';
import { Splash, errors, closeErrors, ROOT } from './lynxkite';

let splash: Splash;
test.beforeAll(async ({ browser }) => {
  splash = await Splash.open(await browser.newPage());
});

test('empty test-example workspace', async function () {
  const workspace = await splash.openNewWorkspace('test-example');
  await workspace.expectCurrentWorkspaceIs('test-example');
  await workspace.close();
});

test('test-example renamed and moved', async function () {
  await splash.expectWorkspaceListed('test-example');

  // Create directory.
  await splash.newDirectory('test-dir');
  await splash.expectWorkspaceNotListed('test-example');

  // Go back and move the project into the directory.
  await splash.popDirectory();
  await splash.expectWorkspaceListed('test-example');
  await splash.renameWorkspace('test-example', 'test-dir/test-example');
  await splash.expectWorkspaceNotListed('test-example');

  // Open directory.
  await splash.openDirectory('test-dir');
  await splash.expectWorkspaceListed('test-example');

  // Rename project.
  await splash.renameWorkspace('test-example', 'test-dir/test-renamed');
  await splash.expectWorkspaceListed('test-renamed');
  await splash.expectWorkspaceNotListed('test-example');

  // Delete project.
  await splash.deleteWorkspace('test-renamed');
  await splash.expectWorkspaceNotListed('test-renamed');

  // Go back and delete the directory.
  await splash.popDirectory();
  await splash.expectWorkspaceNotListed('test-renamed');
  await splash.expectDirectoryListed('test-dir');
  await splash.deleteDirectory('test-dir');
  await splash.expectDirectoryNotListed('test-dir');
});

test('a few workspaces created', async function () {
  // We create this structure:
  // [plum]
  //   [orange]
  //     kiwi
  //   grape
  // apple
  // pear
  let workspace = await splash.openNewWorkspace('apple');
  await workspace.close();
  workspace = await splash.openNewWorkspace('pear');
  await workspace.close();
  await splash.newDirectory('plum');
  workspace = await splash.openNewWorkspace('grape');
  await workspace.close();
  await splash.newDirectory('orange');
  workspace = await splash.openNewWorkspace('kiwi');
  await workspace.close();
  await splash.popDirectory();
  await splash.popDirectory();

  // expect
  await splash.expectWorkspaceListed('apple');
  await splash.expectWorkspaceListed('pear');
  await splash.expectDirectoryListed('plum');
  await splash.expectNumWorkspaces(2);
  await splash.expectNumDirectories(1);

  await splash.openDirectory('plum');
  await splash.expectWorkspaceListed('grape');
  await splash.expectDirectoryListed('orange');
  await splash.expectNumWorkspaces(1);
  await splash.expectNumDirectories(1);

  await splash.openDirectory('orange');
  await splash.expectWorkspaceListed('kiwi');
  await splash.expectNumWorkspaces(1);
  await splash.expectNumDirectories(0);

  await splash.popDirectory();
  await splash.popDirectory();
  await splash.expectWorkspaceListed('apple');
});

test('search works as intended', async function () {
  await splash.enterSearchQuery('a');
  await splash.expectDirectoryListed('orange');
  await splash.expectWorkspaceListed('apple');
  await splash.expectWorkspaceListed('pear');
  await splash.expectWorkspaceListed('grape');
  await splash.expectNumWorkspaces(3);
  await splash.expectNumDirectories(1);

  await splash.enterSearchQuery('a g');
  await splash.expectDirectoryListed('orange');
  await splash.expectWorkspaceListed('grape');
  await splash.expectNumWorkspaces(1);
  await splash.expectNumDirectories(1);

  await splash.enterSearchQuery('kiwi');
  await splash.expectWorkspaceListed('kiwi');
  await splash.expectNumWorkspaces(1);
  await splash.expectNumDirectories(0);

  await splash.enterSearchQuery('orange');
  await splash.expectDirectoryListed('orange');
  await splash.expectNumWorkspaces(0);
  await splash.expectNumDirectories(1);

  await splash.clearSearchQuery();
  await splash.openDirectory('plum');
  await splash.enterSearchQuery('e');
  await splash.expectDirectoryListed('orange');
  await splash.expectWorkspaceListed('grape');
  await splash.expectNumWorkspaces(1);
  await splash.expectNumDirectories(1);
  await splash.popDirectory();
});

test('navigate dirs by clicking on path segment', async function () {
  await splash.newDirectory('red');
  await splash.newDirectory('green');
  await splash.newDirectory('blue');
  await splash.clickBreadcrumb('green');
  await splash.expectDirectoryListed('blue');
  await splash.clickBreadcrumb('red');
  await splash.expectDirectoryListed('green');
  await splash.popDirectory();
});

test('selected directory path does not contain spaces', async function () {
  await splash.newDirectory('first');
  await splash.newDirectory('second');
  await splash.newDirectory('last');
  await splash.expectCurrentDirectory(/first\/second\/last/);
  await splash.popDirectory();
  await splash.popDirectory();
  await splash.popDirectory();
});

test('save workspace as', async () => {
  const workspace = await splash.openWorkspace('apple');
  await workspace.addBox({ id: 'eg', name: 'Create example graph', x: 100, y: 100 });
  await workspace.saveWorkspaceAs('panda');
  // We are now in a workspace with the new name.
  await workspace.expectCurrentWorkspaceIs('panda');
  // We kept the contents of the workspace.
  await expect(workspace.getBox('eg')).toBeVisible();
  // Save in a subdirectory too
  await workspace.saveWorkspaceAs('panda-dir/apple');
  // We are now in a workspace with the new name.
  await workspace.expectCurrentWorkspaceIs('apple');
  // We kept the contents of the workspace.
  await expect(workspace.getBox('eg')).toBeVisible();
  // If the target already exists, we get an error.
  await workspace.saveWorkspaceAs('apple');
  await expect(errors(splash.page)).toHaveText([`Entry '${ROOT}/apple' already exists.`]);
  await closeErrors(splash.page);
  await workspace.close();
  // Check the view from outside.
  await splash.expectCurrentDirectory(/panda-dir/);
  await splash.expectWorkspaceListed('apple');
  await splash.popDirectory();
  await splash.expectWorkspaceListed('apple');
  await splash.expectWorkspaceListed('panda');
});
