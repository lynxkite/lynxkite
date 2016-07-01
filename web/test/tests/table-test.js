'use strict';

module.exports = function(fw) {
  var lib = require('../test-lib.js');
  var path = require('path');
  var columns = 'name,age';
  var tableName = 'csv imported';

  fw.transitionTest(
    'empty splash',
    'CSV file imported as table',
    function() {
      var importPath = path.resolve(__dirname, 'import_csv_test.csv');
      lib.splash.startTableImport();
      lib.splash.importLocalCSVFile(tableName, importPath, columns);
    },
    function() {
      lib.splash.expectNumProjects(0);
      lib.splash.expectNumDirectories(0);
      lib.splash.expectNumTables(1);
      lib.splash.expectTableListed(tableName);
    }
  );
  fw.transitionTest(
    'CSV file imported as table',
    'Project vertices imported from a table',
    function() {
      lib.splash.openNewProject('csv imported project');
      lib.left.runOperation('Import vertices', {table: tableName});
    },
    function() {
      expect(lib.left.vertexCount()).toEqual(3);
      expect(lib.left.attributeCount()).toEqual(3);
    }
  );
  fw.statePreservingTest(
    'CSV file imported as table',
    'Editing imported CSV configuration is possible',
    function() {
      lib.splash.editImport('csv imported');
      expect(element(by.model('tableName')).getAttribute('value')).toEqual(tableName);
      expect(element(by.model('columnsToImport')).getAttribute('value')).toEqual(columns);

      lib.splash.clickAndWaitForImport();
      expect(lib.errors()).toEqual([]);
      lib.splash.expectNumProjects(0);
      lib.splash.expectNumDirectories(0);
      lib.splash.expectNumTables(1);
      lib.splash.expectTableListed(tableName);
    }
  );
};
