'use strict';

module.exports = function() {};

/*
var lib = require('../test-lib.js');

module.exports = function(fw) {
  var numOperations = 5;

  fw.transitionTest(
    'empty test-example project',
    'test-example project with history',
    function() {
      lib.left.runOperation('example graph');
      lib.left.runOperation(
          'compute degree',
           {
            name: 'deg',
            direction: 'incoming edges',
          });
      lib.left.runOperation(
          'add constant vertex attribute',
          {
            name: 'c',
            value: '300000',
          });
      lib.left.runOperation(
          'derive edge attribute',
          {
            output: 'foo',
            expr: 'src$deg + dst$deg',
          });
      lib.left.runOperation(
          'find connected components',
          {
            name: 'connected_components_segmentation',
          });
    },
    function() {
      lib.left.history.open();
      expect(lib.left.side.$('div.project.history').isDisplayed()).
          toBe(true);
      expect(lib.left.history.numOperations()).toBe(numOperations);
      lib.left.history.close();
    });

  fw.statePreservingTest(
    'test-example project with history',
    'valid workflow can be saved (same name)',
    function() {
      lib.left.history.open();
      var op = lib.left.history.getOperation(2);
      lib.left.populateOperation(op, {name: 'new_name'});  // change output name
      lib.left.submitOperation(op);
      lib.left.history.save();
      // Now it is saved and closed. Reopen and check if it's still there.
      lib.left.history.open();
      lib.left.history.expectOperationParameter(2, 'name', 'new_name');
      // Restore original state.
      lib.left.populateOperation(op, {name: 'c'});
      lib.left.submitOperation(op);
      lib.left.history.save();
    });

  fw.statePreservingTest(
    'test-example project with history',
    'valid workflow can be saved (save as...)',
    function() {
      lib.left.history.open();
      var op = lib.left.history.getOperation(2);
      lib.left.populateOperation(op, {name: 'new_output_name'});  // change output name
      lib.left.submitOperation(op);
      lib.left.history.save('changedProjectName');
      // Open new project and verify that the name was changed.
      lib.left.close();
      lib.splash.openProject('changedProjectName');
      lib.left.history.open();
      lib.left.history.expectOperationParameter(2, 'name', 'new_output_name');
      // Go back to the original project and verify that the name was not changed.
      lib.left.history.close();
      lib.left.close();
      lib.splash.deleteProject('changedProjectName');
      lib.splash.openProject('test-example');
      lib.left.history.open();
      lib.left.history.expectOperationParameter(2, 'name', 'c');
      lib.left.history.close();
    });

  fw.statePreservingTest(
    'test-example project with history',
    'save is disabled for unchanged history',
    function() {
      lib.left.history.open();
      lib.left.history.expectSaveable(false);
      lib.left.history.close();
    });

  fw.statePreservingTest(
    'test-example project with history',
    'save is disabled when operations are being edited',
    function() {
      lib.left.history.open();
      var op = lib.left.history.getOperation(2);
      lib.left.populateOperation(op, {name: 'new_output_name'});
      lib.left.history.expectSaveable(false);
      lib.left.history.close(true);
    });

  fw.statePreservingTest(
    'test-example project with history',
    'operation can be deleted from history',
    function() {
      lib.left.history.open();
      lib.left.history.deleteOperation(2);
      expect(lib.left.history.numOperations()).toBe(numOperations - 1);
      expect(lib.left.history.getOperationName(0)).toBe('Create example graph');
      expect(lib.left.history.getOperationName(1)).toBe('Compute degree');
      expect(lib.left.history.getOperationName(2)).toBe('Derive edge attribute');
      expect(lib.left.history.getOperationName(3)).toBe('Find connected components');
      lib.left.history.close(true);
    });

  fw.statePreservingTest(
    'test-example project with history',
    'invalid workflow cannot be saved',
    function() {
      lib.left.history.open();
      lib.left.history.deleteOperation(0);
      expect(lib.left.history.numOperations()).toBe(numOperations - 1);
      expect(lib.left.side.$('.inconsistent-history-sign').isDisplayed()).toBe(true);
      lib.left.history.expectSaveable(false);
      lib.left.history.close(true);
    });

  // TODO(gaborfeher): Also test adding segmentations.
  fw.statePreservingTest(
    'test-example project with history',
    'new operation can be inserted into history (general case)',
    function() {
      lib.left.history.open();
      lib.left.history.insertOperationSimple(
          3, 'Compute PageRank',
          {name: 'wow_such_page_rank'});
      lib.left.history.expectOperationParameter(3, 'name', 'wow_such_page_rank');
      lib.left.history.close(true);
    });

  fw.statePreservingTest(
    'test-example project with history',
    'new operation can be inserted into history (top position)',
    function() {
      lib.left.history.open();
      lib.left.history.insertOperationSimple(
          0, 'Create vertices',
          {size: '111'});
      lib.left.history.expectOperationParameter(0, 'size', '111');
      lib.left.history.close(true);
    });

  fw.statePreservingTest(
    'test-example project with history',
    'new operation can be inserted into history, under a segmentation for segmentation',
    function() {
      lib.left.history.open();
      // Add segmentation operation below and check:
      lib.left.history.insertOperationForSegmentation(
          5,
          'Add gaussian vertex attribute',
          {},
          'connected_components_segmentation');
      expect(lib.left.history.numOperations()).toBe(numOperations + 1);
      expect(lib.left.history.getOperationName(5)).toBe('Add gaussian vertex attribute');
      expect(lib.left.history.getOperationSegmentation(5)).toBe('connected_components_segmentation');

      lib.left.history.close(true);
    });

    fw.statePreservingTest(
      'test-example project with history',
      'new operation can be inserted into history, under a segmentation for whole project',
      function() {
        lib.left.history.open();
        lib.left.history.insertOperationForSegmentation(
            5,
            'Add gaussian vertex attribute',
            {}
            );

        expect(lib.left.history.numOperations()).toBe(numOperations + 1);
        expect(lib.left.history.getOperationName(5)).toBe('Add gaussian vertex attribute');
        expect(lib.left.history.getOperationSegmentation(5)).toBe('');

        lib.left.history.close(true);
      });

  fw.statePreservingTest(
    'test-example project with history',
    'operation type can be changed',
    function() {
      lib.left.history.open();
      var operation = lib.left.history.getOperation(2);
      lib.left.history.enterEditMode(operation);
      lib.left.history.selectOperation(operation, 'Random vertex attribute');
      lib.left.populateOperation(operation, {'seed': '420'});
      lib.left.submitOperation(operation);
      expect(lib.left.history.getOperationName(2)).toBe('Add random vertex attribute');
      lib.left.history.expectOperationParameter(2, 'seed', '420');
      lib.left.history.close(true);
    });

  fw.statePreservingTest(
    'test-example project with history',
    'operation type can be changed and then the change discarded',
    function() {
      lib.left.history.open();
      var operation = lib.left.history.getOperation(2);
      lib.left.history.enterEditMode(operation);
      lib.left.history.selectOperation(operation, 'Random vertex attribute');
      lib.left.populateOperation(operation, {'seed': '420'});
      lib.left.history.discardEdits(operation);
      expect(lib.left.history.getOperationName(2)).toBe('Add constant vertex attribute');
      lib.left.history.close(false);
    });

  fw.statePreservingTest(
    'test-example project with history',
    'operation parameter can be changed',
    function() {
      lib.left.history.open();
      var operation = lib.left.history.getOperation(2);
      lib.left.populateOperation(operation, {'value': '4242'});
      lib.left.submitOperation(operation);
      expect(lib.left.history.getOperationName(2)).toBe('Add constant vertex attribute');
      lib.left.history.expectOperationParameter(2, 'value', '4242');
      lib.left.history.close(true);
    });

  fw.statePreservingTest(
    'test-example project with history',
    'operation parameter can be changed and then the change discarded',
    function() {
      lib.left.history.open();
      var operation = lib.left.history.getOperation(2);
      lib.left.populateOperation(operation, {'value': '4242'});
      lib.left.history.discardEdits(operation);
      expect(lib.left.history.getOperationName(2)).toBe('Add constant vertex attribute');
      lib.left.history.expectOperationParameter(2, 'value', '300000');
      lib.left.history.close(false);
    });

  fw.statePreservingTest(
    'test-example project with history',
    'generate Python code',
    function() {
      lib.left.history.open();
      lib.left.side.$('#toggle-python-button').click();
      expect(lib.getACEText(lib.left.side.$('.python-code'))).toBe(`
project.exampleGraph()
project.degree(direction='incoming edges', name='deg')
project.addConstantVertexAttribute(name='c', type='Double', value=300000)
project.deriveEdgeAttribute(expr='src$deg + dst$deg', output='foo', type='double')
project.connectedComponents(directions='ignore directions', name='connected_components_segmentation')
          `.trim());
      lib.left.history.close(false);
    });

  fw.transitionTest(
      'empty test-example project',
      'test-example project with history with different categories',
      function() {
        lib.left.runOperation('new vertex set', { size: '10' });
        lib.left.runOperation('add random vertex attribute', { seed: 1 });
      },
      function() {
        lib.left.history.open();

        var first = lib.left.history.getOperation(0);
        lib.left.history.enterEditMode(first);
        lib.expectNoClass(lib.left.history.getOperationInCategoryByName(
          first, 'Structure operations', 'Create vertices'
          ), 'disabled');
        lib.expectHasClass(lib.left.history.getOperationInCategoryByName(
          first, 'Vertex attribute operations', 'Add random vertex attribute'
          ), 'disabled');
        lib.left.history.discardEdits(first);

        var second = lib.left.history.getOperation(1);
        lib.left.history.enterEditMode(second);
        lib.expectHasClass(lib.left.history.getOperationInCategoryByName(
          second, 'Structure operations', 'Create vertices'
          ), 'disabled');
        lib.expectNoClass(lib.left.history.getOperationInCategoryByName(
          second, 'Vertex attribute operations', 'Add random vertex attribute'
          ), 'disabled');
        lib.left.history.discardEdits(second);
      });
};
*/
