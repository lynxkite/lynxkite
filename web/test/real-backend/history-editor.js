'use strict';

/* global by */

var lib = require('./test-lib.js');

module.exports = function(fw) {
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
    },
    function() {
      lib.left.historyLib.open();
      expect(lib.left.side.element(by.css('div.project.history')).isDisplayed()).
          toBe(true);
      expect(lib.left.historyLib.numOperations()).toBe(4);
      lib.left.historyLib.close();
    });

  fw.statePreservingTest(
    'test-example project with history',
    'valid workflow can be saved (same name)',
    function() {
      lib.left.historyLib.open();
      var op = lib.left.historyLib.getOperation(2);
      lib.left.populateOperation(op, {name: 'new_name'});  // change operation name
      lib.left.historyLib.save();
      // Now it is saved and closed. Reopen and check if it's still there.
      lib.left.historyLib.open();
      lib.left.historyLib.expectOperationParameter(2, 'name', 'new_name');
      // Restore original state.
      lib.left.populateOperation(op, {name: 'c'});
      lib.left.historyLib.save();
    });

  fw.statePreservingTest(
    'test-example project with history',
    'valid workflow can be saved (save as...)',
    function() {
      lib.left.historyLib.open();
      var op = lib.left.historyLib.getOperation(2);
      lib.left.populateOperation(op, {name: 'new_operation_name'});  // change op. name
      lib.left.historyLib.save('changed_project_name');
      // Open new project and verify that the name was changed.
      lib.left.closeProject();
      lib.left.openProject('changed_project_name');
      lib.left.historyLib.open();
      lib.left.historyLib.expectOperationParameter(2, 'name', 'new_operation_name');
      // Go back to the original project and verify that the name was not changed.
      lib.left.historyLib.close();
      lib.left.closeProject();
      lib.left.openProject('test-example');
      lib.left.historyLib.open();
      lib.left.historyLib.expectOperationParameter(2, 'name', 'c');
      lib.left.historyLib.close();
    });

  fw.statePreservingTest(
    'test-example project with history',
    'save is disabled for unchanged history',
    function() {
      lib.left.historyLib.open();
      lib.left.historyLib.expectSaveable(false);
      lib.left.historyLib.close();
    });

  fw.statePreservingTest(
    'test-example project with history',
    'save is disabled when operations are being edited',
    function() {
      lib.left.historyLib.open();
      var op = lib.left.historyLib.getOperation(2);
      lib.left.populateOperation(op, {name: 'new_operation_name'}, true);
      lib.left.historyLib.expectSaveable(false);
      lib.left.historyLib.close(true);
    });

  fw.statePreservingTest(
    'test-example project with history',
    'operation can be deleted from history',
    function() {
      lib.left.historyLib.open();
      lib.left.historyLib.deleteOperation(2);
      expect(lib.left.historyLib.numOperations()).toBe(3);
      lib.left.historyLib.close(true);
    });

  fw.statePreservingTest(
    'test-example project with history',
    'invalid workflow cannot be saved',
    function() {
      lib.left.historyLib.open();
      lib.left.historyLib.deleteOperation(0);
      expect(lib.left.historyLib.numOperations()).toBe(3);
      expect(lib.left.side.element(by.css('.inconsistent-history-sign')).isDisplayed()).toBe(true);
      lib.left.historyLib.expectSaveable(false);
      lib.left.historyLib.close(true);
    });

  fw.statePreservingTest(
    'test-example project with history',
    'new operation can be inserted into history (below op)',
    function() {
      lib.left.historyLib.open();
      lib.left.historyLib.addOperation(
          2, false, 'PageRank',
          {name: 'wow_such_page_rank'});
      var addedOpNameField = lib.left.historyLib.getOperation(3).element(by.css('div#name input'));
      expect(addedOpNameField.getAttribute('value')).toBe('wow_such_page_rank');
      lib.left.historyLib.close(true);
    });

  fw.statePreservingTest(
    'test-example project with history',
    'new operation can be inserted into history (above op)',
    function() {
      lib.left.historyLib.open();
      lib.left.historyLib.addOperation(
          2, true, 'PageRank',
          {name: 'wow_such_page_rank'});
      var addedOpNameField = lib.left.historyLib.getOperation(2).element(by.css('div#name input'));
      expect(addedOpNameField.getAttribute('value')).toBe('wow_such_page_rank');
      lib.left.historyLib.close(true);
    });

};
