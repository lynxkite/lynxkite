'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'empty test-example workspace',
    'test add constant edge attribute with parametric parameters',
    function() {
      lib.workspace.addBox({id: 'ex0', name: 'create example graph', x: 100, y: 100});
      lib.workspace.addBox({
        id: 'ace', name: 'add constant edge attribute',
        params: {'value': '${1+5}'},
        x: 100, y: 200, after: 'ex0'});
      var boxEditor = lib.workspace.openBoxEditor('ace');
      boxEditor.parametricSwitch('value').click();
      boxEditor.close();
    },
    function() {
      var state = lib.workspace.openStateView('ace', 'project');
      var hist = state.left.edgeAttribute('weight').getHistogramValues();
      var expected = [{title: '6.00-6.00', size: 100, value: 4}];
      expect(hist).toEqual(expected);
      state.close();
    });

  fw.statePreservingTest(
    'test add constant edge attribute with parametric parameters',
    'parametric parameters are preserved',
    function() {
      var boxEditor = lib.workspace.openBoxEditor('ace');
      expect(boxEditor.parametricSwitch('value').getAttribute('class')).toContain('active');
      expect(lib.getACEText(boxEditor.operationParameter('value'))).toBe('${1+5}');
      boxEditor.close();
    });
};
