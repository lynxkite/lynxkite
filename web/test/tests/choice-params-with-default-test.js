'use strict';

module.exports = function() {};

/*
var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'empty test-example project',
    'test pagerank default choice values',
    function() {
      lib.left.runOperation('example graph');
      lib.left.runOperation('pagerank',
          {
            name: 'page_rank_default',
          });
      lib.left.runOperation('pagerank',
          {
            name: 'page_rank_incoming',
            direction: 'incoming edges',
          });
    },
    function() {
      expect(
        lib.left.vertexAttribute('page_rank_incoming').getHistogramValues()).not.toEqual(
        lib.left.vertexAttribute('page_rank_default').getHistogramValues());
      lib.left.history.open();
      lib.left.history.expectOperationSelectParameter(1, 'direction', 'string:outgoing edges');
      lib.left.history.expectOperationSelectParameter(2, 'direction', 'string:incoming edges');
      lib.left.history.close();
    });

  fw.statePreservingTest(
    'test pagerank default choice values',
    'test pagerank default choice values edit history',
    function() {
      lib.left.history.open();
      var op1 = lib.left.history.getOperation(1);
      lib.left.populateOperation(op1, {direction: 'all edges'});  // change direction
      lib.left.submitOperation(op1);
      var op2 = lib.left.history.getOperation(2);
      lib.left.populateOperation(op2, {direction: 'all edges'});  // change direction
      lib.left.submitOperation(op2);
      lib.left.history.save();

      lib.left.history.open();
      lib.left.history.expectOperationSelectParameter(1, 'direction', 'string:all edges');
      lib.left.history.expectOperationSelectParameter(2, 'direction', 'string:all edges');
      // Restore original state.
      var op3 = lib.left.history.getOperation(1);
      lib.left.populateOperation(op3, {direction: 'outgoing edges'});
      lib.left.submitOperation(op3);
      var op4 = lib.left.history.getOperation(2);
      lib.left.populateOperation(op4, {direction: 'incoming edges'});
      lib.left.submitOperation(op4);
      lib.left.history.save();
    });
};
*/
