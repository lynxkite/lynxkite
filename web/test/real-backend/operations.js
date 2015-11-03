'use strict';

var lib = require('./test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'empty test-example project',
    'error reporting',
    function() {
      lib.left.runOperation('Import vertices from CSV files'); // Missing filename.
      // There should be an error message.
      expect(lib.errors()).toMatch(['.*']);
      // Check that we can press OK again. (#2529) (It will give the same error.)
      lib.left.submitOperation(lib.left.toolbox);
      lib.closeErrors();
      lib.left.closeOperation();
    });
  fw.transitionTest(
    'empty test-example project',
    'scalar can show up in an operation box',
    function() {
      lib.left.runOperation('New vertex set', {'size': '10'});
      lib.left.runOperation('Derived vertex attribute', {
        'output': 'seg',
        'type': 'double',
        'expr': 'ordinal % 4'});
      // values of attribute seg now: [0, 0, 0, 1, 1, 1, 2, 2, 3, 3]
      lib.left.runOperation('Segment by double attribute', {
        'attr': 'seg',
        'interval-size': '1',
        'name': 'seg'
      });
      // segment sizes now: [3, 3, 2, 2]
      lib.left.openSegmentation('seg');
      lib.right.openOperation('Create edges from co-occurrence');
    },
    function() {
      lib.right.expectOperationScalar('num_created_edges', '26');
    });
};
