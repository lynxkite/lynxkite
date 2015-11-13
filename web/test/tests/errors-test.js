'use strict';

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'empty test-example project',
    'error in an operation',
    function() {
      lib.left.runOperation('Import vertices from CSV files'); // Missing filename.
      expect(lib.error()).toEqual(
        'File name specifications should always start with a registered prefix (XYZ$)');
      // Check that we can press OK again. (#2529) (It will give the same error.)
      lib.left.submitOperation(lib.left.toolbox);
      lib.closeErrors();
      lib.left.closeOperation();
    });

  fw.transitionTest(
    'test-example project with example graph',
    'error in a scalar',
    function() {
      lib.left.runOperation(
        'Derived vertex attribute', { output: 'empty', 'type': 'double', expr: 'undefined' });
      lib.left.runOperation(
        'Aggregate vertex attribute globally',
        { 'aggregate-empty': 'average,sum', 'aggregate-income': 'average' });
      // Check non-error behavior while we're here.
      expect(lib.left.scalar('income_average').getText()).toBe('2k');
      lib.left.scalar('income_average').click();
      expect(lib.left.scalar('income_average').getText()).toBe('1500');
      expect(lib.left.scalar('empty_sum').getText()).toBe('0');
      // Check error.
      expect(lib.left.scalar('empty_average').getText()).toBe('×');
      lib.left.scalar('empty_average').click();
      lib.expectModal('Error details');
      lib.closeModal();
    }, function() {});
};
