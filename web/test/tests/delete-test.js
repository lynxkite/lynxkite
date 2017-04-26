'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test pagerank default choice values',
    'delete boxes',
    function() {
      lib.workspace.openBoxEditor('ex0').close();
      lib.workspace.openBoxEditor('pr1').close();
      lib.workspace.openBoxEditor('pr2').close();
      lib.workspace.deleteBoxes(['pr1', 'ex0']);
    },
    function() {
      expect(lib.workspace.boxExists('ex0')).toEqual(false);
      expect(lib.workspace.boxExists('pr1')).toEqual(false);
      expect(lib.workspace.boxExists('pr2')).toEqual(true);
    }, 1
  );
};
