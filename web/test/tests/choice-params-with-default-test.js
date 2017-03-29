'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'empty test-example workspace',
    'test pagerank default choice values',
    function() {
      lib.workspace.addBox({id: 'ex0', name: 'create example graph', x: 100, y: 100});
      lib.workspace.addBox({id: 'pr1', name: 'compute pagerank', x: 100, y: 200, after: 'ex0',
                            params: { name: 'page_rank_default' } });
      lib.workspace.addBox({id: 'pr2', name: 'compute pagerank', x: 100, y: 300, after: 'pr1',
                            params: { name: 'page_rank_incoming', direction: 'incoming edges'} });
    },
    function() {
      lib.workspace.getOutputPlug('pr2', 'project').click();
      expect(
        lib.state.vertexAttribute('page_rank_incoming').getHistogramValues()).not.toEqual(
        lib.state.vertexAttribute('page_rank_default').getHistogramValues());
      lib.workspace.selectBox('pr1');
      lib.workspace.expectSelectedBoxSelectParameter('direction', 'string:outgoing edges');
      lib.workspace.selectBox('pr2');
      lib.workspace.expectSelectedBoxSelectParameter('direction', 'string:incoming edges');
    });

  fw.statePreservingTest(
    'test pagerank default choice values',
    'test pagerank default choice values edit workspace',
    function() {
      lib.workspace.editBox('pr1', {direction: 'all edges'});  // change direction
      lib.workspace.editBox('pr2', {direction: 'all edges'});  // change direction

      lib.workspace.selectBox('pr1');
      lib.workspace.expectSelectedBoxSelectParameter('direction', 'string:all edges');
      lib.workspace.selectBox('pr2');
      lib.workspace.expectSelectedBoxSelectParameter('direction', 'string:all edges');

      // Restore original state, because this is a state-preserving test.
      lib.workspace.editBox('pr1', {direction: 'outgoing edges'});
      lib.workspace.editBox('pr2', {direction: 'incoming edges'});
    });
};
