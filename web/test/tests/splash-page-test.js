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
      browser.get('/');
    },
    function() {
    });

  fw.transitionTest(
    'empty splash',
    'empty test-example project',
    function() {
      lib.splash.openNewProject('test-example');
    },
    function() {
      lib.left.expectCurrentProjectIs('test-example');
    });

  fw.transitionTest(
    'empty test-example project',
    'test-example renamed and moved',
    function() {
      // Go to project list.
      lib.left.close();
      lib.splash.expectProjectListed('test-example');

      // Create directory.
      lib.splash.newDirectory('test-dir');
      lib.splash.expectProjectNotListed('test-example');

      // Go back and move the project into the directory.
      lib.splash.popDirectory();
      lib.splash.expectProjectListed('test-example');
      lib.splash.renameProject('test-example', 'test-dir/test-example');
      lib.splash.expectProjectNotListed('test-example');

      // Open directory.
      lib.splash.openDirectory('test-dir');
      lib.splash.expectProjectListed('test-example');

      // Rename project.
      lib.splash.renameProject('test-example', 'test-dir/test-renamed');
      lib.splash.expectProjectListed('test-renamed');
      lib.splash.expectProjectNotListed('test-example');

      // Delete project.
      lib.splash.deleteProject('test-renamed');
      lib.splash.expectProjectNotListed('test-renamed');

      // Go back and delete the directory.
      lib.splash.popDirectory();
      lib.splash.expectProjectNotListed('test-renamed');
      lib.splash.expectDirectoryListed('test-dir');
      lib.splash.deleteDirectory('test-dir');
      lib.splash.expectDirectoryNotListed('test-dir');
    },
    function() {
    });

  fw.transitionTest(
    'empty splash',
    'a few projects created',
    function() {
      // We create this structure:
      // [plum]
      //   [orange]
      //     kiwi
      //   grape
      // apple
      // pear
      lib.splash.openNewProject('apple');
      lib.left.close();
      lib.splash.openNewProject('pear');
      lib.left.close();
      lib.splash.newDirectory('plum');
      lib.splash.openNewProject('grape');
      lib.left.close();
      lib.splash.newDirectory('orange');
      lib.splash.openNewProject('kiwi');
      lib.left.close();
      lib.splash.popDirectory();
      lib.splash.popDirectory();
    },
    function() {
      lib.splash.expectProjectListed('apple');
      lib.splash.expectProjectListed('pear');
      lib.splash.expectDirectoryListed('plum');
      lib.splash.expectNumProjects(2);
      lib.splash.expectNumDirectories(1);

      lib.splash.openDirectory('plum');
      lib.splash.expectProjectListed('grape');
      lib.splash.expectDirectoryListed('orange');
      lib.splash.expectNumProjects(1);
      lib.splash.expectNumDirectories(1);

      lib.splash.openDirectory('orange');
      lib.splash.expectProjectListed('kiwi');
      lib.splash.expectNumProjects(1);
      lib.splash.expectNumDirectories(0);

      lib.splash.popDirectory();
      lib.splash.popDirectory();
      lib.splash.expectProjectListed('apple');
    });

  fw.statePreservingTest(
    'a few projects created',
    'search works as intended',
    function() {
      lib.splash.enterSearchQuery('a');
      lib.splash.expectDirectoryListed('plum/orange');
      lib.splash.expectProjectListed('apple');
      lib.splash.expectProjectListed('pear');
      lib.splash.expectProjectListed('plum/grape');
      lib.splash.expectNumProjects(3);
      lib.splash.expectNumDirectories(1);

      lib.splash.enterSearchQuery('a g');
      lib.splash.expectDirectoryListed('plum/orange');
      lib.splash.expectProjectListed('plum/grape');
      lib.splash.expectNumProjects(1);
      lib.splash.expectNumDirectories(1);

      lib.splash.enterSearchQuery('kiwi');
      lib.splash.expectProjectListed('plum/orange/kiwi');
      lib.splash.expectNumProjects(1);
      lib.splash.expectNumDirectories(0);

      lib.splash.enterSearchQuery('orange');
      lib.splash.expectDirectoryListed('plum/orange');
      lib.splash.expectNumProjects(0);
      lib.splash.expectNumDirectories(1);

      lib.splash.clearSearchQuery();
      lib.splash.openDirectory('plum');
      lib.splash.enterSearchQuery('e');
      lib.splash.expectDirectoryListed('orange');
      lib.splash.expectProjectListed('grape');
      lib.splash.expectNumProjects(1);
      lib.splash.expectNumDirectories(1);
      lib.splash.popDirectory();
    });
};
