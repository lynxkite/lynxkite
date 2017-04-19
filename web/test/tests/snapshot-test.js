'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {

  var snapshotName = 'This is a snapshot.';

  fw.transitionTest(
    'test-example workspace with example graph state selected',
    'snapshot-created',
    function() {
      var inputBox = $$('.save-as-snapshot-box input');
      var inputButton = $$('.save-as-snapshot-box .glyphicon-camera');

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
      lib.workspace.addBox({id: 'sb0', name: 'load snapshot',
                            params: {path: snapshotName},
                            x: 100, y: 100});
    },
    function() {
      expect(lib.state.vertexCount()).toEqual(4);
      expect(lib.state.edgeCount()).toEqual(4);
      expect(lib.state.attributeCount()).toEqual(8);
    });

};
