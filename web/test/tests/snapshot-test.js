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
      var inputBox = state.popup
          .$('.save-as-snapshot-box input');
      var inputButton = state.popup
          .$('.save-as-snapshot-box .glyphicon-camera');

      inputBox.sendKeys(lib.selectAllKey + snapshotName);
      inputButton.click();
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
        id: 'sb0', name: 'load snapshot', params: {path: snapshotName}, x: 300, y: 100});
    },
    function() {
      var state = lib.workspace.openStateView('sb0', 'project');
      expect(state.left.vertexCount()).toEqual(4);
      expect(state.left.edgeCount()).toEqual(4);
      expect(state.left.attributeCount()).toEqual(8);
      state.close();
    });

};
