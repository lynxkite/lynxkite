'use strict';

/* global by */

var lib = require('./test-lib.js');

module.exports = function(fw) {
  var numOperations = 5;

  fw.transitionTest(
    'empty test-example project',
    'test-example project with history',
    function() {
      lib.left.runOperation('example graph');
      lib.left.runOperation(
          'degree',
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
          'derived edge attribute',
          {
            output: 'foo',
            expr: 'src$deg + dst$deg',
          });
      lib.left.runOperation(
          'connected components',
          {
            name: 'connected_components_segmentation',
          });
    },
    function() {
      lib.left.history.open();
      expect(lib.left.side.element(by.css('div.project.history')).isDisplayed()).
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
      lib.left.history.save('changed_project_name');
      // Open new project and verify that the name was changed.
      lib.left.close();
      lib.splash.openProject('changed_project_name');
      lib.left.history.open();
      lib.left.history.expectOperationParameter(2, 'name', 'new_output_name');
      // Go back to the original project and verify that the name was not changed.
      lib.left.history.close();
      lib.left.close();
      lib.splash.openProject('test-example');
      lib.left.history.open();
      lib.left.history.expectOperationParameter(2, 'name', 'c');
      lib.left.history.close();
      // TODO(gaborfeher): Delete new project.
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
      expect(lib.left.history.getOperationName(0)).toBe('Example Graph');
      expect(lib.left.history.getOperationName(1)).toBe('Degree');
      expect(lib.left.history.getOperationName(2)).toBe('Derived edge attribute');
      expect(lib.left.history.getOperationName(3)).toBe('Connected components');
      lib.left.history.close(true);
    });

  fw.statePreservingTest(
    'test-example project with history',
    'invalid workflow cannot be saved',
    function() {
      lib.left.history.open();
      lib.left.history.deleteOperation(0);
      expect(lib.left.history.numOperations()).toBe(numOperations - 1);
      expect(lib.left.side.element(by.css('.inconsistent-history-sign')).isDisplayed()).toBe(true);
      lib.left.history.expectSaveable(false);
      lib.left.history.close(true);
    });

  // TODO(gaborfeher): Also test adding segmentations.
  fw.statePreservingTest(
    'test-example project with history',
    'new operation can be inserted into history (below op)',
    function() {
      lib.left.history.open();
      lib.left.history.insertOperation(
          2, 'down', 'PageRank',
          {name: 'wow_such_page_rank'});
      var addedOpNameField = lib.left.history.getOperation(3).element(by.css('div#name input'));
      expect(addedOpNameField.getAttribute('value')).toBe('wow_such_page_rank');
      lib.left.history.close(true);
    });

  fw.statePreservingTest(
    'test-example project with history',
    'new operation can be inserted into history (above op)',
    function() {
      lib.left.history.open();
      lib.left.history.insertOperation(
          2, 'up', 'PageRank',
          {name: 'wow_such_page_rank'});
      var addedOpNameField = lib.left.history.getOperation(2).element(by.css('div#name input'));
      expect(addedOpNameField.getAttribute('value')).toBe('wow_such_page_rank');
      lib.left.history.close(true);
    });

  fw.statePreservingTest(
    'test-example project with history',
    'new operation can be inserted into history, under a segmentation',
    function() {
      lib.left.history.open();

      // Add segmentation operation below and check:
      lib.left.history.insertOperation(
          4,
          'down',
          'Add gaussian vertex attribute',
          {},
          'connected_components_segmentation');
      expect(lib.left.history.numOperations()).toBe(numOperations + 1);
      expect(lib.left.history.getOperationName(5)).toBe('Add gaussian vertex attribute');
      expect(lib.left.history.getOperationSegmentation(5)).toBe('connected_components_segmentation');

      // Add segmentation operation above and check:
      lib.left.history.insertOperation(
          5,
          'up',
          'Add constant vertex attribute',
          {name: 'const_attr'},
          'connected_components_segmentation');
      expect(lib.left.history.numOperations()).toBe(numOperations + 2);
      expect(lib.left.history.getOperationName(5)).toBe('Add constant vertex attribute');
      expect(lib.left.history.getOperationSegmentation(5)).toBe('connected_components_segmentation');
      expect(lib.left.history.getOperationName(6)).toBe('Add gaussian vertex attribute');
      expect(lib.left.history.getOperationSegmentation(6)).toBe('connected_components_segmentation');

      lib.left.history.close(true);
    });

};
