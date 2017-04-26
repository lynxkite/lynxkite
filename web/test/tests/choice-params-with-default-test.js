'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'empty test-example workspace',
    'test pagerank default choice values',
    function() {
      lib.workspace.addBox({id: 'ex0', name: 'create example graph', x: 100, y: 100});
      lib.workspace.addBox({
        id: 'pr1', name: 'compute pagerank', x: 100, y: 200, after: 'ex0',
        params: { name: 'page_rank_default' } });
      lib.workspace.addBox({
        id: 'pr2', name: 'compute pagerank', x: 100, y: 300, after: 'pr1',
        params: { name: 'page_rank_incoming', direction: 'incoming edges'} });
    },
    function() {
      var state = lib.workspace.openStateView('pr2', 'project');
      expect(
        state.left.vertexAttribute('page_rank_incoming').getHistogramValues()).not.toEqual(
        state.left.vertexAttribute('page_rank_default').getHistogramValues());
      state.close();
      var boxEditor = lib.workspace.openBoxEditor('pr1');
      boxEditor.expectSelectParameter('direction', 'string:outgoing edges');
      boxEditor.close();
      boxEditor = lib.workspace.openBoxEditor('pr2');
      boxEditor.expectSelectParameter('direction', 'string:incoming edges');
      boxEditor.close();
    });

  fw.statePreservingTest(
    'test pagerank default choice values',
    'test pagerank default choice values edit workspace',
    function() {
      lib.workspace.editBox('pr1', {direction: 'all edges'});  // change direction
      lib.workspace.editBox('pr2', {direction: 'all edges'});  // change direction
      var boxEditor = lib.workspace.openBoxEditor('pr1');
      boxEditor.expectSelectParameter('direction', 'string:all edges');
      boxEditor.close();
      boxEditor = lib.workspace.openBoxEditor('pr2');
      boxEditor.expectSelectParameter('direction', 'string:all edges');
      boxEditor.close();

      // Restore original state, because this is a state-preserving test.
      lib.workspace.editBox('pr1', {direction: 'outgoing edges'});
      lib.workspace.editBox('pr2', {direction: 'incoming edges'});
    });
};
