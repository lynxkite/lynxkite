'use strict';

module.exports = function(fw) {
  var lib = require('../test-lib.js');
  var path = require('path');
  var columns = 'name,age';
  var viewName = 'csv view';
  var jdbcViewName = 'jdbc view';
  var jdbcImportPath = path.resolve(__dirname, 'data/import_jdbc_test.sqlite');
  var jdbcUrl = 'jdbc:sqlite:' + jdbcImportPath;

  fw.transitionTest(
    'empty splash',
    'CSV file imported as view',
    function() {
      var importPath = path.resolve(__dirname, 'data/import_csv_test.csv');
      lib.splash.startTableImport();
      lib.splash.importLocalCSVFile(
          viewName, importPath, columns, columns, true);
    },
    function() {
      lib.splash.expectNumProjects(0);
      lib.splash.expectNumDirectories(0);
      lib.splash.expectNumTables(0);
      lib.splash.expectNumViews(1);
      lib.splash.expectViewListed(viewName);
    }
  );

  fw.transitionTest(
    'CSV file imported as view',
    'View described in global SQL box',
    function() {
      lib.splash.view(viewName).click();
    },
    function() {
      var expectedData = [['Adam', '24'], ['Eve', '32'], ['Bob', '41']];
      lib.splash.expectGlobalSqlResult(columns.split(','), ['String', 'String'], expectedData);
    }
  );

  fw.statePreservingTest(
    'CSV file imported as view',
    'CSV file edited',
    function() {
      lib.splash.editView(viewName);
      expect(element(by.css('#table-name input')).getAttribute('value')).toEqual(viewName);
      expect(element(by.css('#csv-column-names input')).getAttribute('value')).toEqual(columns);
      expect(element(by.css('#columns-to-import input')).getAttribute('value')).toEqual(columns);
    }
  );

  fw.statePreservingTest(
    'CSV file imported as view',
    'Global sql box returns results for CSV view',
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
  fw.transitionTest(
    'empty splash',
    'Sqlite file imported via JDBC as view',
    function() {
      lib.splash.startTableImport();
      lib.splash.importJDBC(jdbcViewName, jdbcUrl, 'table1', 'a', true);
      expect(lib.errors()).toEqual([]);
    },
    function() {
      lib.splash.expectNumProjects(0);
      lib.splash.expectNumDirectories(0);
      lib.splash.expectNumTables(0);
      lib.splash.expectNumViews(1);
      lib.splash.expectViewListed(jdbcViewName);
    });
  fw.statePreservingTest(
    'Sqlite file imported via JDBC as view',
    'Global sql box returns results for JDBC view',
    function() {
      lib.splash.runGlobalSql('select * from `' + jdbcViewName + '`');
      lib.startDownloadWatch();
      lib.splash.saveGlobalSqlToCSV();
      var downloadedFileName = lib.waitForNewDownload(/\.csv$/);
      lib.expectFileContents(
        downloadedFileName,
        'a,b\n' +
        '1,2\n' +
        '1,3\n' +
        '42,42\n');
    }
  );
  fw.transitionTest(
  'empty splash',
  'Global sql view can be edited',
  function() {
    lib.splash.runGlobalSql('select 2 as n');
    lib.splash.saveGlobalSqlToView('view');
    lib.splash.editView('view');
    element(by.id('save-results-opener')).click();
    element(by.id('save-results')).click();
  },
  function() {
    expect(lib.errors()).toEqual([]);
    lib.splash.expectNumProjects(0);
    lib.splash.expectNumDirectories(0);
    lib.splash.expectNumViews(1);
  });
};
