'use strict';

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    undefined,
    'empty splash',
    function() {
      lib.discardAll();
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
      lib.expectCurrentProjectIs('test-example');
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
      lib.splash.moveProject('test-example', 'test-dir');
      lib.splash.expectProjectNotListed('test-example');

      // Open directory.
      lib.splash.openDirectory('test-dir');
      lib.splash.expectProjectListed('test-example');

      // Rename project.
      lib.splash.renameProject('test-example', 'test-renamed');
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
};
