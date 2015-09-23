'use strict';

/* global element, by  */

module.exports = function(fw) {
  var lib = require('./test-lib.js');
  var vertexCSVPathOnDisk = '/tmp/export_vertex_attributes_csv.csv';
  var fs = require('fs');

  function cleanUp(path) {
    if (fs.existsSync(path)) {
      fs.unlinkSync(path);
    }
  }
  
  fw.transitionTest(
    'test-example project with example graph',
    'export example graph vertex attributes',
    function() {
      cleanUp(vertexCSVPathOnDisk);
      lib.left.runOperation('Export vertex attributes to file', {attrs: 'name'});
      var e = element(by.css('#scalar-vertex_attributes_csv'));
      e.click();
    },
    function() {
      browser.driver.wait(function() {
        return fs.existsSync(vertexCSVPathOnDisk);
      }, 10000);
     });
};
