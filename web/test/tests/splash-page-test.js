'use strict';

var lib = require('../test-lib.js');
var fs = require('fs');

module.exports = function(fw) {
  fw.transitionTest(
    undefined,
    'empty splash',
    function() {
      lib.discardAll();
      if (fs.existsSync(lib.protractorDownloads)) {
        fs.readdirSync(lib.protractorDownloads).map(function(file) {
          fs.unlinkSync(lib.protractorDownloads + '/' + file);
        });
        fs.rmdirSync(lib.protractorDownloads);
      }
      fs.mkdirSync(lib.protractorDownloads);
      browser.get('/#/');
    },
    function() {
    });

  fw.transitionTest(
    'empty splash',
    'empty test-example workspace',
    function() {
      lib.splash.openNewWorkspace('test-example');
    },
    function() {
      lib.workspace.expectCurrentWorkspaceIs('test-example');
    });

  fw.transitionTest(
    'empty test-example workspace',
    'test-example renamed and moved',
    function() {
      // Go to project list.
      lib.workspace.close();
      lib.splash.expectWorkspaceListed('test-example');

      // Create directory.
      lib.splash.newDirectory('test-dir');
      lib.splash.expectWorkspaceNotListed('test-example');

      // Go back and move the project into the directory.
      lib.splash.popDirectory();
      lib.splash.expectWorkspaceListed('test-example');
      lib.splash.renameWorkspace('test-example', 'test-dir/test-example');
      lib.splash.expectWorkspaceNotListed('test-example');

      // Open directory.
      lib.splash.openDirectory('test-dir');
      lib.splash.expectWorkspaceListed('test-example');

      // Rename project.
      lib.splash.renameWorkspace('test-example', 'test-dir/test-renamed');
      lib.splash.expectWorkspaceListed('test-renamed');
      lib.splash.expectWorkspaceNotListed('test-example');

      // Delete project.
      lib.splash.deleteWorkspace('test-renamed');
      lib.splash.expectWorkspaceNotListed('test-renamed');

      // Go back and delete the directory.
      lib.splash.popDirectory();
      lib.splash.expectWorkspaceNotListed('test-renamed');
      lib.splash.expectDirectoryListed('test-dir');
      lib.splash.deleteDirectory('test-dir');
      lib.splash.expectDirectoryNotListed('test-dir');
    },
    function() {
    });

  fw.transitionTest(
    'empty splash',
    'a few workspaces created',
    function() {
      // We create this structure:
      // [plum]
      //   [orange]
      //     kiwi
      //   grape
      // apple
      // pear
      lib.splash.openNewWorkspace('apple');
      lib.workspace.close();
      lib.splash.openNewWorkspace('pear');
      lib.workspace.close();
      lib.splash.newDirectory('plum');
      lib.splash.openNewWorkspace('grape');
      lib.workspace.close();
      lib.splash.newDirectory('orange');
      lib.splash.openNewWorkspace('kiwi');
      lib.workspace.close();
      lib.splash.popDirectory();
      lib.splash.popDirectory();
    },
    function() {
      lib.splash.expectWorkspaceListed('apple');
      lib.splash.expectWorkspaceListed('pear');
      lib.splash.expectDirectoryListed('plum');
      lib.splash.expectNumWorkspaces(2);
      lib.splash.expectNumDirectories(1);

      lib.splash.openDirectory('plum');
      lib.splash.expectWorkspaceListed('grape');
      lib.splash.expectDirectoryListed('orange');
      lib.splash.expectNumWorkspaces(1);
      lib.splash.expectNumDirectories(1);

      lib.splash.openDirectory('orange');
      lib.splash.expectWorkspaceListed('kiwi');
      lib.splash.expectNumWorkspaces(1);
      lib.splash.expectNumDirectories(0);

      lib.splash.popDirectory();
      lib.splash.popDirectory();
      lib.splash.expectWorkspaceListed('apple');
    });

  fw.statePreservingTest(
    'a few projects created',
    'search works as intended',
    function() {
      lib.splash.enterSearchQuery('a');
      lib.splash.expectDirectoryListed('plum/orange');
      lib.splash.expectWorkspaceListed('apple');
      lib.splash.expectWorkspaceListed('pear');
      lib.splash.expectWorkspaceListed('plum/grape');
      lib.splash.expectNumWorkspaces(3);
      lib.splash.expectNumDirectories(1);

      lib.splash.enterSearchQuery('a g');
      lib.splash.expectDirectoryListed('plum/orange');
      lib.splash.expectWorkspaceListed('plum/grape');
      lib.splash.expectNumWorkspaces(1);
      lib.splash.expectNumDirectories(1);

      lib.splash.enterSearchQuery('kiwi');
      lib.splash.expectWorkspaceListed('plum/orange/kiwi');
      lib.splash.expectNumWorkspaces(1);
      lib.splash.expectNumDirectories(0);

      lib.splash.enterSearchQuery('orange');
      lib.splash.expectDirectoryListed('plum/orange');
      lib.splash.expectNumWorkspaces(0);
      lib.splash.expectNumDirectories(1);

      lib.splash.clearSearchQuery();
      lib.splash.openDirectory('plum');
      lib.splash.enterSearchQuery('e');
      lib.splash.expectDirectoryListed('orange');
      lib.splash.expecWorkspaceListed('grape');
      lib.splash.expectNumWorkspaces(1);
      lib.splash.expectNumDirectories(1);
      lib.splash.popDirectory();
    });
};
