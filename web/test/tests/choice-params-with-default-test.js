'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'empty test-example workspace',
    'test pagerank default choice values',
    function() {
      lib.workspace.addBox('create example graph', 100, 100, 'ex0', []);
      lib.workspace.addBox('compute pagerank', 100, 200, 'pr1', [{
        boxID: 'ex0',
        srcPlugID: 'project',
        dstPlugID: 'project'
      }]);
      lib.workspace.addBox('compute pagerank', 100, 300, 'pr2', [{
        boxID: 'pr1',
        srcPlugID: 'project',
        dstPlugID: 'project'
      }]);
      lib.workspace.editBox(
          'pr1',
          {
            name: 'page_rank_default',
          });
      lib.workspace.editBox(
          'pr2',
          {
            name: 'page_rank_incoming',
            direction: 'incoming edges',
          });
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
