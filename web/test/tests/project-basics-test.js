'use strict';


const lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test-example workspace with example graph',
    'test-example workspace with example graph saved as apple',
    function() {
      lib.workspace.saveWorkspaceAs('apple');
    },
    function() {
      // We are now in a workspace with the new name.
      lib.workspace.expectCurrentWorkspaceIs('apple');
      // We also kept the contents of the workspace.
      // The example graph box was created with boxId=eg0.
      expect(lib.workspace.boxExists('eg0')).toEqual(true);
    });

  fw.transitionTest(
    'test-example workspace with example graph saved as apple',
    'test-example workspace with example graph saved as pear/apple',
    function() {
      lib.workspace.saveWorkspaceAs('pear/apple');
    },
    function() {
      // We are now in a workspace with the new name.
      lib.workspace.expectCurrentWorkspaceIs('apple');
      // We also kept the contents of the project.
      expect(lib.workspace.boxExists('eg0')).toEqual(true);
    });

  fw.statePreservingTest(
    'test-example workspace with example graph saved as apple',
    'cant save as to existing workspace',
    function() {
      lib.workspace.saveWorkspaceAs('test-example');
      expect(lib.error()).toEqual('Entry \'test-example\' already exists.');
      lib.closeErrors();
    });
};
