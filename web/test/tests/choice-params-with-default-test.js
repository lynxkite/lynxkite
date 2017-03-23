'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'empty test-example workspace',
    'test pagerank default choice values',
    function() {
      lib.workspace.addBox('create example graph', 100, 100);
      var ex0 = lib.workspace.getBox(0);
      lib.workspace.addBox('compute pagerank', 100, 200);
      var pr1 = lib.workspace.getBox(1);
      lib.workspace.addBox('compute pagerank', 100, 300);
      var pr2 = lib.workspace.getBox(2);
      lib.workspace.connectBoxes(ex0, 'project', pr1, 'project');
      lib.workspace.connectBoxes(pr1, 'project', pr2, 'project');
      lib.workspace.editBox(
          pr1,
          {
            name: 'page_rank_default',
          });
      lib.workspace.editBox(
          pr2,
          {
            name: 'page_rank_incoming',
            direction: 'incoming edges',
          });
    },
    function() {
      var pr1 = lib.workspace.getBox(1);
      var pr2 = lib.workspace.getBox(2);
      lib.workspace.getOutputPlug(pr2, 'project').click();
      expect(
        lib.state.vertexAttribute('page_rank_incoming').getHistogramValues()).not.toEqual(
        lib.state.vertexAttribute('page_rank_default').getHistogramValues());
      lib.workspace.selectBox(pr1);
      lib.workspace.expectSelectedBoxSelectParameter('direction', 'string:outgoing edges');
      lib.workspace.selectBox(pr2);
      lib.workspace.expectSelectedBoxSelectParameter('direction', 'string:incoming edges');
    });

  fw.statePreservingTest(
    'test pagerank default choice values',
    'test pagerank default choice values edit workspace',
    function() {
      var pr1 = lib.workspace.getBox(1);
      var pr2 = lib.workspace.getBox(2);
      lib.workspace.editBox(pr1, {direction: 'all edges'});  // change direction
      lib.workspace.editBox(pr2, {direction: 'all edges'});  // change direction

      lib.workspace.selectBox(pr1);
      lib.workspace.expectSelectedBoxSelectParameter('direction', 'string:all edges');
      lib.workspace.selectBox(pr2);
      lib.workspace.expectSelectedBoxSelectParameter('direction', 'string:all edges');

      // Restore original state, because this is a state-preserving test.
      lib.workspace.editBox(pr1, {direction: 'outgoing edges'});
      lib.workspace.editBox(pr2, {direction: 'incoming edges'});
    });
};
