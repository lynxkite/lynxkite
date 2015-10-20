'use strict';

var lib = require('./test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'empty test-example project',
    'error reporting',
    function() {
      lib.left.runOperation('Import vertices from CSV files'); // Missing filename.
      var expectedMessage = 'File name specifications should always start with a registered prefix (XYZ$)';
      expect(lib.errors()).toEqual([expectedMessage]);
      // Check that we can try submitting again. #2529
      lib.left.submitOperation(lib.left.toolbox);
      lib.closeErrors();
      lib.left.closeOperation();
    });
};
