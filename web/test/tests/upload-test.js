'use strict';

module.exports = function() {};

/*
module.exports = function(fw) {
  var lib = require('../test-lib.js');
  var path = require('path');
  var importPath = path.resolve(__dirname, 'data/upload_test.csv');

  fw.transitionTest(
    'empty test-example project',
    'example graph vertex set names imported',
    function() {
      lib.left.runOperation('Import vertices', { table: importPath });
    },
    function() {
      expect(lib.left.vertexCount()).toEqual(4);
    }
  );
};
*/
