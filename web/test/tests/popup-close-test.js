'use strict';

const lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'test pagerank default choice values',
    'test popup closing order with escape',
    function() {
      const state = lib.workspace.openStateView('pr2', 'graph');
      const boxEditor = lib.workspace.openBoxEditor('pr1');
      lib.workspace.closeLastPopup();
      expect(boxEditor.isPresent()).toBeFalsy();
      lib.workspace.closeLastPopup();
      expect(state.popup.isPresent()).toBeFalsy();
    });
};
