'use strict';

const lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'empty test-example workspace',
    'test-example workspace with example graph',
    function() {
      lib.workspace.addBox({ id: 'eg0', name: 'Create example graph', x: 100, y: 100 });
    },
    function() {
    });

  fw.statePreservingTest(
    'test-example workspace with example graph',
    'has the proper vertex count',
    function() {
      const state = lib.workspace.openStateView('eg0', 'graph');
      expect(state.left.vertexCount()).toEqual(4);
      expect(state.left.edgeCount()).toEqual(4);
      expect(state.left.attributeCount()).toEqual(8);
      state.close();
    });

  fw.transitionTest(
    'test-example workspace with example graph',
    'test-example workspace with example graph state open',
    function() {
      lib.workspace.openStateView('eg0', 'graph');
    },
    function() {
    });

  fw.transitionTest(
    'test-example workspace with example graph',
    'test-example workspace with reverse edges',
    function() {
      lib.workspace.addBox({ id: 'eg1', name: 'Create example graph', x: 350, y: 100 });
      lib.workspace.addBox({
        id: 'reversed-edges', name: 'Add reversed edges', x: 100, y: 200, after: 'eg0' });
    },
    function() {
    });

  fw.statePreservingTest(
    'test-example workspace with reverse edges',
    'has the proper vertex count',
    function() {
      const state = lib.workspace.openStateView(
        'reversed-edges', 'graph');
      expect(state.left.vertexCount()).toEqual(4);
      expect(state.left.edgeCount()).toEqual(8);
      expect(state.left.attributeCount()).toEqual(8);
      state.close();
    });

  fw.statePreservingTest(
    'test-example workspace with reverse edges',
    'plugs have color',
    function() {
      let outPlugFirstEG = lib.workspace.getOutputPlug('eg0');
      let outPlugReversedEdges = lib.workspace.getOutputPlug('reversed-edges');
      let outPlugSecondEG = lib.workspace.getOutputPlug('eg1');
      expect(outPlugFirstEG.getAttribute('class').then(cls =>
        cls.indexOf('plug-progress-unknown') === -1)).toBeTruthy();
      expect(outPlugReversedEdges.getAttribute('class').then(cls =>
        cls.indexOf('plug-progress-unknown') === -1)).toBeTruthy();
      expect(outPlugSecondEG.getAttribute('class').then(cls =>
        cls.indexOf('plug-progress-unknown') === -1)).toBeTruthy();
    });

};
