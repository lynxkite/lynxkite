'use strict';

/* global element, by  */

module.exports = function(fw) {
  var lib = require('./test-lib.js');
  var downloadName = lib.getTheRandomPathPrefix() + '_vertex_attributes_csv';
  var exportPrefix = 'export_';
  var fullDownloadName = '/tmp/' + exportPrefix + downloadName + '.csv';
  var fs = require('fs');

  fw.transitionTest(
    'test-example project with example graph',
    'export example graph vertex attributes',
    function() {
      lib.left.runOperation('Export vertex attributes to file',
                            {attrs: 'name', link: downloadName});
      var e = element(by.css('#scalar-' + downloadName));
      e.click();
    },
    function() {
      browser.driver.wait(function() {
        return fs.existsSync(fullDownloadName);
      }, 10000);
     });
};
