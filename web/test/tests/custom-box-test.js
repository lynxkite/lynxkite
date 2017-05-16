'use strict';

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'empty splash',
    'custom box created',
    function() {
      lib.splash.openNewWorkspace('test-custom-box');
      lib.workspace.addWorkspaceParameter('prname', 'text', 'default_pr');
      lib.workspace.addBox({
        id: 'in', name: 'Input', x: 100, y: 100, params: { name: 'in' } });
      lib.workspace.addBox({
        id: 'eg', name: 'Create example graph', x: 400, y: 100 });
      lib.workspace.addBox({
        id: 'pr', name: 'Compute PageRank', x: 100, y: 300, after: 'eg' });
      var pr = lib.workspace.openBoxEditor('pr');
      pr.populateOperation({ name: '$prname' });
      pr.parametricSwitch('name').click();
      pr.close();
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
      expect(state.left.vertexAttribute('default_pr').isPresent()).toBe(true);
      expect(state.left.vertexAttribute('clustering_coefficient').isPresent()).toBe(true);
      state.close();
    });

  fw.transitionTest(
    'custom box placed',
    'custom box with parameter set',
    function() {
      lib.workspace.editBox('cb', { prname: 'custom_pr' });
      var state = lib.workspace.openStateView('cb', 'out');
      expect(state.left.vertexAttribute('default_pr').isPresent()).toBe(false);
      expect(state.left.vertexAttribute('custom_pr').isPresent()).toBe(true);
      state.close();
    },
    function() {});

  fw.transitionTest(
    'custom box with parameter set',
    'dive into custom box',
    function() {
      lib.workspace.selectBox('cb');
      $('#dive-down').click();
      var state = lib.workspace.openStateView('cc', 'project');
      expect(state.left.vertexAttribute('custom_pr').isPresent()).toBe(true);
      expect(state.left.vertexAttribute('clustering_coefficient').isPresent()).toBe(true);
      state.close();

      // Make a change.
      var cc = lib.workspace.openBoxEditor('cc');
      cc.populateOperation({ name: 'clustco' });
      cc.close();
      state = lib.workspace.openStateView('cc', 'project');
      expect(state.left.vertexAttribute('clustering_coefficient').isPresent()).toBe(false);
      expect(state.left.vertexAttribute('clustco').isPresent()).toBe(true);
      state.close();

      // Affects the higher level too.
      $('#dive-up').click();
      state = lib.workspace.openStateView('cb', 'out');
      expect(state.left.vertexAttribute('clustering_coefficient').isPresent()).toBe(false);
      expect(state.left.vertexAttribute('clustco').isPresent()).toBe(true);
      state.close();
    },
    function() {});

};
