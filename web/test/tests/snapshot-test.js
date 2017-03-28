'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'test-example workspace with example graph state selected',
    'snapshot-created',
    function() {
      var inputBox = $$('.save-as-snapshot-box input');
      var inputButton = $$('.save-as-snapshot-box .glyphicon-camera');

      var name = 'This is a snapshot.';
      inputBox.sendKeys(lib.selectAllKey + name);
      inputButton.click();
      lib.workspace.close();

      lib.splash.expectSnapshotListed(name);
    });
};
