'use strict';

const lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'empty test-example workspace',
    'test pagerank default choice values',
    function() {
      lib.workspace.addBox({id: 'ex0', name: 'Create example graph', x: 100, y: 100});
      lib.workspace.addBox({
        id: 'pr1', name: 'Compute PageRank', x: 100, y: 200, after: 'ex0',
        params: { name: 'page_rank_default', iterations: '1' } });
      lib.workspace.addBox({
        id: 'pr2', name: 'Compute PageRank', x: 100, y: 300, after: 'pr1',
        params: { name: 'page_rank_incoming', direction: 'incoming edges', iterations: '1' } });
    },
    function() {
      const state = lib.workspace.openStateView('pr2', 'graph');
      expect(
        state.left.vertexAttribute('page_rank_incoming').getHistogramValues()).not.toEqual(
        state.left.vertexAttribute('page_rank_default').getHistogramValues());
      state.close();
      let boxEditor = lib.workspace.openBoxEditor('pr1');
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
      lib.workspace.editBox('pr1', {direction: 'all edges'}); // change direction
      lib.workspace.editBox('pr2', {direction: 'all edges'}); // change direction
      let boxEditor = lib.workspace.openBoxEditor('pr1');
      boxEditor.expectSelectParameter('direction', 'string:all edges');
      boxEditor.close();
      boxEditor = lib.workspace.openBoxEditor('pr2');
      boxEditor.expectSelectParameter('direction', 'string:all edges');
      boxEditor.close();

      // Restore original state, because this is a state-preserving test.
      lib.workspace.editBox('pr1', {direction: 'outgoing edges'});
      lib.workspace.editBox('pr2', {direction: 'incoming edges'});
    });

  fw.transitionTest(
    'empty test-example workspace',
    'multi-select',
    function() {
      lib.workspace.addBox({id: 'ex', name: 'Create example graph', x: 100, y: 100});
      lib.workspace.addBox({
        id: 'discard', name: 'Discard vertex attributes', x: 100, y: 200, after: 'ex'});
      const box = lib.workspace.openBoxEditor('discard').moveTo(500, 100);
      const state = lib.workspace.openStateView('discard', 'graph').moveTo(500, 400);
      function attrs() {
        return state.left.side.$$('entity[kind="vertex-attribute"]').map(e => e.getText());
      }
      expect(attrs()).toEqual(['age', 'gender', 'id', 'income', 'location', 'name']);
      box.populateOperation({ name: ['income'] });
      expect(attrs()).toEqual(['age', 'gender', 'id', 'location', 'name']);
      box.populateOperation({ name: ['income', 'location'] });
      expect(attrs()).toEqual(['age', 'gender', 'id', 'name']);
      box.populateOperation({ name: [] });
      expect(attrs()).toEqual(['age', 'gender', 'id', 'income', 'location', 'name']);
    },
    function() {});
};
