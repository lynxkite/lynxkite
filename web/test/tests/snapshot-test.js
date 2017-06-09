'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {

  var snapshotName = 'This is a snapshot.';

  fw.transitionTest(
    'test-example workspace with example graph',
    'snapshot-created',
    function() {
      var state = lib.workspace.openStateView('eg0', 'project');
      var snapshotBox = state.popup.$('#save-as-snapshot-box');
      var snapshotButton = snapshotBox.$('#save-as-snapshot-button');

      snapshotButton.click();
      lib.submitInlineInput(snapshotBox, snapshotName);
      lib.workspace.close();
    },
    function() {
      lib.splash.expectSnapshotListed(snapshotName);
    });

  fw.transitionTest(
    'snapshot-created',
    'snapshot-loaded-in-new-workspace',
    function() {
      lib.splash.openNewWorkspace('test-load-snapshot');
      lib.workspace.addBox({
        id: 'sb0', name: 'Load snapshot', params: {path: snapshotName}, x: 100, y: 100});
    },
    function() {
      var state = lib.workspace.openStateView('sb0', 'project');
      expect(state.left.vertexCount()).toEqual(4);
      expect(state.left.edgeCount()).toEqual(4);
      expect(state.left.attributeCount()).toEqual(8);
      state.close();
    });

};
