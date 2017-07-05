'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'test pagerank default choice values',
    'select boxes with mouse drag',
    () => {
      lib.workspace.selectArea('pr2', {x: -5, y: 30}, {x: 200, y: -5000});
      lib.workspace.expectNumSelectedBoxes(4);
    });
};
