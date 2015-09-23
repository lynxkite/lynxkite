'use strict';


module.exports = function(fw) {
  var lib = require('./test-lib.js');
  var importPath = '/tmp/import_vertex_attributes_csv.csv';
  var fs = require('fs');

  function write(path) {
    var handle = fs.openSync(path, 'w');
    fs.writeSync(handle, '\"name\"\n\"Adam\"\n\"Eve\"\n\"Bob\"\n\"Isolated Joe\"\n', 0, 'UTF-8');
    fs.closeSync(handle);
  }
  
  fw.transitionTest(
    'empty test-example project',
    'example graph vertex set names imported',    
    function() {
      write(importPath);
      lib.left.openOperation('Import vertices from CSV files');
      lib.left.upload(importPath);
      lib.left.clickOperationOk();
    },
    function() {
      expect(lib.left.vertexCount()).toEqual(4);
    }
  );
};
