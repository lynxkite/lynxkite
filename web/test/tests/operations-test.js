'use strict';

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'test-example project with example graph',
    'attributes are alphabetically ordered',
    function() {
      lib.left.openOperation('Convert vertex attribute to string');
      var list = lib.left.operationParameter(lib.left.toolbox, 'attr');
      var expected = ['age', 'gender', 'id', 'income', 'location', 'name'];
      expect(list.getText()).toEqual(expected.join('\n'));
      lib.left.closeOperation();
    });
  fw.transitionTest(
    'empty test-example project',
    'scalar can show up in an operation box',
    function() {
      lib.left.runOperation('Create vertices', { size: '10' });
      lib.left.runOperation('Convert vertex attribute to double', { attr: 'ordinal' });
      lib.left.runOperation('Derive vertex attribute', {
        'output': 'seg',
        'type': 'double',
        'expr': 'ordinal % 4'});
      // values of attribute seg now: [0, 0, 0, 1, 1, 1, 2, 2, 3, 3]
      lib.left.runOperation('Segment by double attribute', {
        'attr': 'seg',
        'interval_size': '1',
        'name': 'seg'
      });
      // segment sizes now: [3, 3, 2, 2]
      lib.left.openSegmentation('seg');
      lib.right.openOperation('Create edges from co-occurrence');
      lib.right.expectOperationScalar('num_created_edges', '? \u21bb');
      lib.right.toolbox.$('.value-retry').click();
      lib.right.expectOperationScalar('num_created_edges', '26');
    },
    function() {
    });
};
