'use strict';

var lib = require('./test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'example graph with filters applied',
    'we don\'t have a saved workflow yet',
    function() {
      expect(lib.left.getCategorySelector('User Defined Workflows').isPresent()).toBe(false);
    });
  fw.transitionTest(
    'example graph with filters applied',
    'example graph simple history',
    function() {
      lib.left.historyLib.open();
    },
    function() {
      expect(lib.left.getProjectHistory().isDisplayed()).toBe(true);
    });
  fw.transitionTest(
    'example graph simple history',
    'workflow dialog with simple example graph history',
    function() {
      lib.left.openWorkflowSavingDialog();
    },
    function() {
      expect(lib.left.getWorkflowCodeEditor().evaluate('code')).toBe(
        'project.exampleGraph()' +
          '\nproject.filterByAttributes(\'filterea-comment\': \'\',' +
          ' \'filterea-weight\': \'!1\',' +
          ' \'filterva-age\': \'<40\',' +
          ' \'filterva-gender\': \'\',' +
          ' \'filterva-id\': \'\',' +
          ' \'filterva-income\': \'\',' +
          ' \'filterva-location\': \'\',' +
          ' \'filterva-name\': \'Adam,Eve,Bob\')');
    });
  fw.transitionTest(
    'workflow dialog with simple example graph history',
    'filtered example graph with saved workflow',
    function() {
      lib.sendKeysToElement(
        lib.left.getWorkflowNameEditor(),
        lib.selectAllKey + 'TestDeriveWorkflow');
      lib.sendKeysToElement(
        lib.left.getWorkflowDescriptionEditor(),
        lib.selectAllKey + 'A simple workflow that does a\nsimple derive on\na simple project.');
      lib.sendKeysToACE(
        lib.left.getWorkflowCodeEditor(),
        lib.selectAllKey +
          'project.derivedVertexAttribute' +
          '(expr: \'gender == \\\'Male\\\' ? \\\'Mr \\\' + name : \\\'Ms \\\' + name\',' +
          ' output: \'polite\',' +
          ' type: \'string\')');
      lib.left.getWorkflowSaveButton().click();
      lib.left.getCloseHistoryButton().click();
    },
    function() {
      expect(lib.left.getCategorySelector('User Defined Workflows').isPresent()).toBe(true);
    });
  fw.transitionTest(
    'filtered example graph with saved workflow',
    'example graph with saved workflow executed',
    function() {
      lib.left.undoButton().click();
      lib.left.runOperation('TestDeriveWorkflow');
    },
    function() {
      expect(lib.left.vertexCount()).toEqual(4);
      expect(lib.left.edgeCount()).toEqual(4);
      expect(lib.left.attributeCount()).toEqual(9);
    });

  fw.statePreservingTest(
    'example graph with saved workflow executed',
    'test workflow\'s result looks good',
    function() {
      expect(lib.left.getHistogramValues('polite').then(lib.sortHistogramValues)).toEqual([
        { title: 'Mr Adam', size: 100, value: 1 },
        { title: 'Mr Bob', size: 100, value: 1 },
        { title: 'Mr Isolated Joe', size: 100, value: 1 },
        { title: 'Ms Eve', size: 100, value: 1 },
      ]);
    });
};
