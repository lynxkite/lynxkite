'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test pagerank default choice values',
    'copy paste boxes',
    function() {
      lib.workspace.selectBoxes(['pr1', 'ex0']);
      lib.workspace.duplicate();
    },
    function() {
      lib.workspace.selectBox('Create-example-graph_1'); // Fails if there is no box
      lib.workspace.selectBox('Compute-PageRank_1');
      lib.workspace.expectSelectedBoxSelectParameter('direction', 'string:outgoing edges');
    });
};
