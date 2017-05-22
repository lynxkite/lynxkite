'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test pagerank default choice values',
    'delete boxes',
    function() {
      lib.workspace.clickBox('pr2'); // To check that only popups for deleted boxes are closed.
      lib.workspace.deleteBoxes(['pr1', 'ex0']);
    },
    function() {
      expect(lib.workspace.boxExists('ex0')).toEqual(false);
      expect(lib.workspace.boxExists('pr1')).toEqual(false);
      expect(lib.workspace.boxExists('pr2')).toEqual(true);
      expect(lib.workspace.boxPopupExists('ex0')).toEqual(false);
      expect(lib.workspace.boxPopupExists('pr1')).toEqual(false);
      expect(lib.workspace.boxPopupExists('pr2')).toEqual(true);
    }
  );
};
