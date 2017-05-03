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
      boxEditor.toggleParametricFlag('value');
      boxEditor.close();
    },
    function() {
//      var boxEditor = lib.workspace.openBoxEditor('ace');
      var state = lib.workspace.openStateView('ace', 'project');
      var hist = state.left.edgeAttribute('weight').getHistogramValues();
      var expected = [{title: '6.00-6.00', size: 100, value: 4}];
//      browser.pause();
      expect(hist).toEqual(expected);
//      boxEditor.close();
      state.close();
    });

  fw.statePreservingTest(
    'test add constant edge attribute with parametric parameters',
    'parametric parameters are preserved',
    function() {
      var boxEditor = lib.workspace.openBoxEditor('ace');
      boxEditor.isParametric('value');
      browser.pause();
      boxEditor.close();
    }, true);
};
