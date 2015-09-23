'use strict';

/* global element, by  */


module.exports = function(fw) {
  var lib = require('./test-lib.js');
  var path = require('path');
  var importPath = path.resolve(__dirname, 'upload_test.csv');

  fw.transitionTest(
    'empty test-example project',
    'example graph vertex set names imported',
    function() {
      lib.left.openOperation('Import vertices from CSV files');
      var e = element(by.css('input[type=file]'));
      lib.setParameter(e, importPath);
      lib.left.clickOperationOk();
    },
    function() {
      expect(lib.left.vertexCount()).toEqual(4);
    }
  );
};
