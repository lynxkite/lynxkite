'use strict';

module.exports = function(fw) {
  var lib = require('../test-lib.js');
  var path = require('path');
  var columns = 'name,age';
  var viewName = 'csv view';

  fw.transitionTest(
    'empty splash',
    'CSV file imported as view',
    function() {
      var importPath = path.resolve(__dirname, 'import_csv_test.csv');
      lib.splash.startTableImport();
      lib.splash.importLocalCSVFile(viewName, importPath, columns, true);
    },
    function() {
      lib.splash.expectNumProjects(0);
      lib.splash.expectNumDirectories(0);
      lib.splash.expectNumTables(0);
      lib.splash.expectNumViews(1);
      lib.splash.expectViewListed(viewName);
    }
  );
  fw.statePreservingTest(
    'CSV file imported as view',
    'Global sql box returns results',
    function() {
      lib.splash.runGlobalSql('select * from `' + viewName + '`');
      lib.startDownloadWatch();
      lib.splash.saveGlobalSqlToCSV();
      var downloadedFileName = lib.waitForNewDownload(/\.csv$/);
      lib.expectFileContents(
        downloadedFileName,
        'name,age\n' +
        'Adam,24\n' +
        'Eve,32\n' +
        'Bob,41\n');
    }
  );
};
