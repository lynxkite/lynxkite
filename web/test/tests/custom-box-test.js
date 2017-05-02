'use strict';

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'empty splash',
    'custom box created',
    function() {
      lib.splash.openNewWorkspace('test-custom-box');
      lib.workspace.addBox({
        id: 'in', name: 'Input', x: 100, y: 100, params: { name: 'in' } });
      lib.workspace.addBox({
        id: 'eg', name: 'Create example graph', x: 400, y: 100 });
      lib.workspace.addBox({
        id: 'pr', name: 'Compute PageRank', x: 100, y: 300, after: 'eg' });
      lib.workspace.addBox({
        id: 'cc', name: 'Compute clustering coefficient', x: 100, y: 400, after: 'pr' });
      lib.workspace.addBox({
        id: 'out', name: 'Output', x: 100, y: 500 });
      lib.workspace.connectBoxes('cc', 'project', 'out', 'output');
      lib.workspace.editBox('out', { name: 'out' });
      lib.workspace.connectBoxes('in', 'input', 'pr', 'project');
      lib.workspace.close();
    },
    function() {});

  fw.transitionTest(
    'custom box created',
    'custom box placed',
    function() {
      lib.splash.openNewWorkspace('test-example');
      lib.workspace.addBox({
        id: 'eg', name: 'Create example graph', x: 100, y: 100 });
      lib.workspace.addBox({
        id: 'cb', name: 'test-custom-box', x: 100, y: 200 });
      lib.workspace.connectBoxes('eg', 'project', 'cb', 'in');
    },
    function() {});

  fw.statePreservingTest(
    'custom box placed',
    'check custom box output',
    function() {
      var state = lib.workspace.openStateView('cb', 'out');
      expect(state.left.vertexCount()).toEqual(4);
      expect(state.left.edgeCount()).toEqual(4);
      expect(state.left.attributeCount()).toEqual(10);
      state.close();
    });

};
