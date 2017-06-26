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
      lib.workspace.openBoxEditor('create-example-graph_1').close(); // Fails if there is no box
      var pr = lib.workspace.openBoxEditor('compute-pagerank_1');
      pr.expectSelectParameter('direction', 'string:outgoing edges');
    });
};
