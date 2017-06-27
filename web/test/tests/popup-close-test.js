'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'test pagerank default choice values',
    'test popup closing order with escape',
    function() {
      var state = lib.workspace.openStateView('pr2', 'project');
      var boxEditor = lib.workspace.openBoxEditor('pr1');
      lib.workspace.closeLastPopup();
      expect(boxEditor.isPresent()).toBeFalsy();
      lib.workspace.closeLastPopup();
      expect(state.popup.isPresent()).toBeFalsy();
    });
};
