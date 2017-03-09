'use strict';

var fs = require('fs');
var lib = require('../test-lib.js');
var K = protractor.Key;

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
      expect(lib.getACEText(lib.left.getWorkflowCodeEditor())).toBe(`
project.exampleGraph()
project.filterByAttributes('filterea_weight': '!1', 'filterva_age': '<40', 'filterva_name': 'Adam,Eve,Bob')
      `.trim());
      lib.left.closeWorkflowSavingDialog();
      lib.left.history.close();
    });

    fw.transitionTest(
      'test-example project with example graph',
      'example graph with age attribute renamed and python editor opened',
      function() {
        lib.left.side.element(by.id('vertex-attribute-age')).click();
        var e = lib.left.vertexAttribute('age');
        var popup = e.popup();
        popup.element(by.id('rename-menu-item')).click();
        var inputField = popup.element(by.id('rename-to'));
        inputField.sendKeys(lib.selectAllKey + 'newage');
        inputField.sendKeys(K.ENTER);
        lib.left.history.open();
        lib.left.side.element(by.id('toggle-python-button')).click();
        expect(lib.getACEText(lib.left.getPythonWorkflowCodeEditor())).toBe(`
project.exampleGraph()
project.renameVertexAttribute(before='age', after='newage')
        `.trim());
      },
      function() {}
    );

  fw.transitionTest(
    'empty test-example project',
    'some project is open',
    // It is okay to leak workflows and modify the project in this state.
    function() {},
    function() {
      // Just make sure we return to the main project view.
      lib.expectElement(lib.left.side);
      lib.expectNotElement(lib.left.getProjectHistory());
      expect(lib.errors()).toEqual([]);
    });

  function tryToSaveWorkflow(name, description, code) {
    lib.left.history.open();
    lib.left.openWorkflowSavingDialog();
    lib.left.getWorkflowNameEditor().sendKeys(lib.selectAllKey + name);
    lib.left.getWorkflowDescriptionEditor().sendKeys(lib.selectAllKey + description);
    lib.sendKeysToACE(
      lib.left.getWorkflowCodeEditor(),
      lib.selectAllKey + code);
    lib.left.getWorkflowSaveButton().click();
  }

  function saveWorkflow(name, description, code) {
    tryToSaveWorkflow(name, description, code);
    lib.left.history.close();
  }

  fw.statePreservingTest(
    'some project is open',
    'simple derived attribute workflow',
    function() {
      saveWorkflow(
        'TestDeriveWorkflow',
        'A simple workflow that does a\nsimple derive on\na simple project.',
        `
project.exampleGraph()
project.deriveVertexAttribute(expr: 'gender == \\'Male\\' ? \\'Mr \\' + name : \\'Ms \\' + name', output: 'polite', type: 'string')
        `.trim());
      lib.left.runOperation('TestDeriveWorkflow');
      expect(lib.left.vertexCount()).toEqual(4);
      expect(lib.left.edgeCount()).toEqual(4);
      expect(lib.left.attributeCount()).toEqual(9);
      var polite = lib.left.vertexAttribute('polite');
      expect(polite.getHistogramValues().then(lib.sortHistogramValues)).toEqual([
        { title: 'Mr Adam', size: 100, value: 1 },
        { title: 'Mr Bob', size: 100, value: 1 },
        { title: 'Mr Isolated Joe', size: 100, value: 1 },
        { title: 'Ms Eve', size: 100, value: 1 },
      ]);
    });

  fw.statePreservingTest(
    'some project is open',
    'complex workflow with parameters and "if"',
    function() {
      saveWorkflow(
        'ComplexTest',
        'The workflow example from the help page.',
        fs.readFileSync('app/help/project-ui/workflow-example.groovy', 'utf8'));
      lib.left.runOperation('ComplexTest', { size: 25, degree: 'all edges' });
      expect(lib.left.vertexCount()).toEqual(25);
      expect(lib.left.edgeCount()).toEqual(191);
      expect(lib.left.attributeCount()).toEqual(4);
      var count = lib.left.vertexAttribute('neighborhood_count-of-all-edges_max');
      expect(count.getHistogramValues()).toEqual([
         { title : '57.00-57.15', size : 44, value : 4 },
         { title : '57.15-57.30', size : 0, value : 0 },
         { title : '57.30-57.45', size : 0, value : 0 },
         { title : '57.45-57.60', size : 0, value : 0 },
         { title : '57.60-57.75', size : 0, value : 0 },
         { title : '57.75-57.90', size : 0, value : 0 },
         { title : '57.90-58.05', size : 0, value : 0 },
         { title : '58.05-58.20', size : 0, value : 0 },
         { title : '58.20-58.35', size : 0, value : 0 },
         { title : '58.35-58.50', size : 0, value : 0 },
         { title : '58.50-58.65', size : 0, value : 0 },
         { title : '58.65-58.80', size : 0, value : 0 },
         { title : '58.80-58.95', size : 0, value : 0 },
         { title : '58.95-59.10', size : 0, value : 0 },
         { title : '59.10-59.25', size : 0, value : 0 },
         { title : '59.25-59.40', size : 0, value : 0 },
         { title : '59.40-59.55', size : 0, value : 0 },
         { title : '59.55-59.70', size : 0, value : 0 },
         { title : '59.70-59.85', size : 0, value : 0 },
         { title : '59.85-60.00', size : 100, value : 9 },
      ]);
      // TODO: Try loading the saved visualization. (Instead of the histogram maybe?)
    });

  fw.statePreservingTest(
    'some project is open',
    'malicious workflow trying to print to the console',
    function() {
      saveWorkflow('MaliciousTest1', '', 'print "hello world"');
      lib.left.runOperation('MaliciousTest1');
      expect(lib.error()).toMatch(
        'java.lang.SecurityException: Script tried to execute a disallowed operation');
      lib.closeErrors();
      lib.left.closeOperation();
    });

  fw.statePreservingTest(
    'some project is open',
    'malicious workflow trying to get a classloader',
    function() {
      saveWorkflow('MaliciousTest2', '', '"hello world".getClass().getClassLoader()');
      lib.left.runOperation('MaliciousTest2');
      expect(lib.error()).toMatch(
        'java.lang.SecurityException: Script tried to execute a disallowed operation');
      lib.closeErrors();
      lib.left.closeOperation();
    });

  fw.statePreservingTest(
    'some project is open',
    'syntax error in workflow',
    function() {
      tryToSaveWorkflow('SyntaxErrorTest', '', '}{');
      expect(lib.error()).toMatch('unexpected token');
      lib.closeErrors();
      lib.left.closeWorkflowSavingDialog();
      lib.left.history.close();
    });

  fw.statePreservingTest(
    'some project is open',
    'editing a workflow',
    function() {
      saveWorkflow(
        'TestConstWorkflow',
        'A simple workflow that adds a constant attribute.',
        'project.exampleGraph()\n');
      lib.left.openOperation('TestConstWorkflow');
      lib.left.clickWorkflowEditButton();
      lib.sendKeysToACE(
        lib.left.getWorkflowCodeEditor(),
        K.chord(K.CONTROL, K.END) + K.ENTER +
        `project.addConstantVertexAttribute(
          name: 'test-const-attr', value: '1.0', type: 'Double')`);
      lib.left.getWorkflowSaveButton().click();
      var testConstAttr = lib.left.vertexAttribute('test-const-attr');
      lib.expectNotElement(testConstAttr);
      lib.left.runOperation('TestConstWorkflow');
      lib.expectElement(testConstAttr);
    });
};
