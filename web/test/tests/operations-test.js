'use strict';

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'test-example project with example graph',
    'attributes are alphabetically ordered',
    function() {
      lib.left.openOperation('Vertex attribute to string');
      var list = lib.left.operationParameter(lib.left.toolbox, 'attr');
      var expected = ['age', 'gender', 'id', 'income', 'location', 'name'];
      expect(list.getText()).toEqual(expected.join('\n'));
      lib.left.closeOperation();
    });
};
