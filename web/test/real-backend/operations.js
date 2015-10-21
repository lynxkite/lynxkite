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
};
