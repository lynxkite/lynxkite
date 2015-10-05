'use strict';

var lib = require('./test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'example graph with filters applied',
    'we don\'t have a saved workflow yet',
    function() {
      expect(lib.left.getCategorySelector('User Defined Workflows').isPresent()).toBe(false);
    });

  fw.statePreservingTest(
    'example graph with filters applied',
    'workflow dialog with simple example graph history',
    function() {
      lib.left.history.open();
      lib.left.openWorkflowSavingDialog();
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
      lib.left.history.close();
    });

  fw.transitionTest(
    'example graph with filters applied',
    'unspecified project state for testing user-defined workflows',
    // It is okay to leak workflows and modify the project in this state.
    function() {},
    function() {
      // Just make sure we return to the main project view.
      expect(lib.left.getProjectHistory().isDisplayed()).toBe(false);
    });

  function saveWorkflow(name, description, code) {
    lib.left.history.open();
    lib.left.openWorkflowSavingDialog();
    lib.left.getWorkflowNameEditor().sendKeys(lib.selectAllKey + name);
    lib.left.getWorkflowDescriptionEditor().sendKeys(lib.selectAllKey + description);
    lib.sendKeysToACE(
      lib.left.getWorkflowCodeEditor(),
      lib.selectAllKey + code);
    lib.left.getWorkflowSaveButton().click();
    lib.left.history.close();
  }

  fw.statePreservingTest(
    'example graph with user-defined workflows',
    'simple derived attribute workflow',
    function() {
      saveWorkflow(
        'TestDeriveWorkflow',
        'A simple workflow that does a\nsimple derive on\na simple project.',
        'project.derivedVertexAttribute' +
          '(expr: \'gender == \\\'Male\\\' ? \\\'Mr \\\' + name : \\\'Ms \\\' + name\',' +
          ' output: \'polite\',' +
          ' type: \'string\')');
      lib.left.runOperation('TestDeriveWorkflow');
      expect(lib.left.vertexCount()).toEqual(4);
      expect(lib.left.edgeCount()).toEqual(4);
      expect(lib.left.attributeCount()).toEqual(9);
      expect(lib.left.getHistogramValues('polite').then(lib.sortHistogramValues)).toEqual([
        { title: 'Mr Adam', size: 100, value: 1 },
        { title: 'Mr Bob', size: 100, value: 1 },
        { title: 'Mr Isolated Joe', size: 100, value: 1 },
        { title: 'Ms Eve', size: 100, value: 1 },
      ]);
    });
};
