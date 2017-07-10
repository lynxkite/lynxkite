'use strict';

var lib = require('../test-lib.js');

module.exports = function(fw) {
  function setParametric(boxId, param, value) {
    var editor = lib.workspace.openBoxEditor(boxId);
    var params = {};
    params[param] = value;
    editor.populateOperation(params);
    editor.parametricSwitch(param).click();
    editor.close();
  }

  fw.transitionTest(
    'empty splash',
    'custom box created',
    function() {
      lib.splash.openNewWorkspace('test-custom-box');
      lib.workspace.addWorkspaceParameter('prname', 'text', 'default_pr');
      lib.workspace.addBox({
        id: 'in', name: 'Input', x: 100, y: 0, params: { name: 'in' } });
      lib.workspace.addBox({
        id: 'eg', name: 'Create example graph', x: 400, y: 0 });
      lib.workspace.addBox({
        id: 'pr', name: 'Compute PageRank', x: 100, y: 100, after: 'eg' });
      setParametric('pr', 'name', '$prname');
      lib.workspace.addBox({
        id: 'cc', name: 'Compute clustering coefficient', x: 100, y: 200, after: 'pr' });
      lib.workspace.addBox({
        id: 'out', name: 'Output', x: 100, y: 300 });
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

  fw.transitionTest(
    'empty splash',
    'save as custom box with parameters',
    function() {
      // Set up a few connected boxes.
      lib.splash.openNewWorkspace('test-custom-box');
      lib.workspace.addWorkspaceParameter('prname', 'text', 'default_pr');
      lib.workspace.addBox({
        id: 'eg', name: 'Create example graph', x: 100, y: 100 });
      lib.workspace.addBox({
        id: 'pr1', name: 'Compute PageRank', x: 100, y: 200, after: 'eg' });
      setParametric('pr1', 'name', '${prname}_1');
      lib.workspace.addBox({
        id: 'pr2', name: 'Compute PageRank', x: 100, y: 300, after: 'pr1' });
      setParametric('pr2', 'name', '${prname}_2');
      lib.workspace.addBox({
        id: 'pr3', name: 'Compute PageRank', x: 100, y: 400, after: 'pr2' });
      setParametric('pr3', 'name', '${prname}_3');

      function checkOutput() {
        var state = lib.workspace.openStateView('pr3', 'project');
        expect(state.left.vertexAttribute('default_pr_1').isPresent()).toBe(true);
        expect(state.left.vertexAttribute('default_pr_2').isPresent()).toBe(true);
        expect(state.left.vertexAttribute('default_pr_3').isPresent()).toBe(true);
        state.close();
      }
      checkOutput();

      // Now save "pr1" and "pr2" as a custom box.
      lib.workspace.selectBoxes(['pr1', 'pr2']);
      $('#save-selection-as-custom-box').click();
      lib.submitInlineInput($('#save-selection-as-custom-box-input'), 'my-custom-box');

      // Check that the box has been replaced.
      expect(lib.workspace.getBox('pr1').isPresent()).toBe(false);
      expect(lib.workspace.getBox('pr2').isPresent()).toBe(false);
      expect(lib.workspace.getBox('eg').isPresent()).toBe(true);
      expect(lib.workspace.getBox('pr3').isPresent()).toBe(true);

      // The output is still the same.
      checkOutput();
    },
    function() {});

  fw.transitionTest(
    'empty splash',
    'save as custom box with mixed outputs',
    function() {
      lib.splash.openNewWorkspace('test-custom-box');
      lib.workspace.addBox({
        id: 'eg', name: 'Create example graph', x: 0, y: 200 });
      lib.workspace.addBox({
        id: 'pr1', name: 'Compute PageRank', x: 200, y: 100, after: 'eg' });
      lib.workspace.addBox({
        id: 'pr2', name: 'Compute PageRank', x: 200, y: 200, after: 'eg' });
      lib.workspace.addBox({
        id: 'pr3', name: 'Compute PageRank', x: 400, y: 200, after: 'pr2' });

      lib.workspace.selectBoxes(['eg', 'pr2']);
      $('#save-selection-as-custom-box').click();
      lib.submitInlineInput($('#save-selection-as-custom-box-input'), 'my-custom-box');
      lib.workspace.expectConnected('my-custom-box_1', 'project', 'pr1', 'project');
      lib.workspace.expectConnected('my-custom-box_1', 'project-2', 'pr3', 'project');
    },
    function() {});

  fw.transitionTest(
    'empty splash',
    'browse-custom-box',
    function() {
      lib.splash.newDirectory('browse-custom-box-dir');
      lib.splash.openNewWorkspace('browse-custom-box-ws');

      lib.workspace.addBox({
        id: 'eg1', name: 'Create example graph', x: 0, y: 200 });
      lib.workspace.selectBoxes(['eg1']);
      $('#save-selection-as-custom-box').click();
      lib.submitInlineInput($('#save-selection-as-custom-box-input'),
        'my-custom-box-to-browse-1');

      lib.workspace.addBox({
        id: 'eg2', name: 'Create example graph', x: 0, y: 400 });
      lib.workspace.selectBoxes(['eg2']);
      $('#save-selection-as-custom-box').click();
      lib.submitInlineInput($('#save-selection-as-custom-box-input'),
        'browse-custom-box-dir/my-custom-box-to-browse-2');

      // Check top level elements.
      var root = lib.workspace.getCustomBoxBrowserTree();
      var dir = root.$('#browse-custom-box-dir');
      lib.expectElement(dir);
      lib.expectElement(root.$('#my-custom-box-to-browse-1'));

      // Test that the custom box in the dir is only present after click.
      lib.expectNotElement(dir.$('#my-custom-box-to-browse-2'));
      dir.click();
      lib.expectElement(dir.$('#my-custom-box-to-browse-2'));
    },
    function() {});
};
