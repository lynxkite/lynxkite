'use strict';

const lib = require('../test-lib.js');

module.exports = function(fw) {

  const snapshotName = 'This is a snapshot!';

  fw.transitionTest(
    'test-example workspace with example graph',
    'snapshot created',
    function() {
      const state = lib.workspace.openStateView('eg0', 'graph');
      const snapshotBox = state.popup.$('#save-as-snapshot-box');
      const snapshotButton = state.popup.$('#save-as-snapshot-button');

      snapshotButton.click();
      lib.submitInlineInput(snapshotBox, snapshotName);
      lib.workspace.close();
    },
    function() {
      lib.splash.expectSnapshotListed(snapshotName);
    });

  fw.transitionTest(
    'snapshot created',
    'snapshot loaded in new workspace',
    function() {
      lib.splash.openNewWorkspace('test-load-snapshot');
      lib.workspace.addBox({
        id: 'sb0', name: 'Import snapshot', params: {path: snapshotName}, x: 100, y: 100});
    },
    function() {
      const state = lib.workspace.openStateView('sb0', 'state');
      expect(state.left.vertexCount()).toEqual(4);
      expect(state.left.edgeCount()).toEqual(4);
      expect(state.left.attributeCount()).toEqual(8);
      state.close();
    });

  fw.transitionTest(
    'snapshot created',
    'snapshot can be viewed in workspace browser',
    function() {
      lib.splash.snapshot(snapshotName).click();
    },
    function() {
      const state = lib.viewerState(snapshotName);
      expect(state.left.vertexCount()).toEqual(4);
      expect(state.left.edgeCount()).toEqual(4);
      expect(state.left.attributeCount()).toEqual(8);
    });

};
