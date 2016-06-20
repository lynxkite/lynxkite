'use strict';

module.exports = function(fw) {
  var lib = require('../test-lib.js');
  var path = require('path');

  fw.transitionTest(
    'empty splash',
    'CSV file imported as table',
    function() {
      var importPath = path.resolve(__dirname, 'import_csv_test.csv');
      lib.splash.startTableImport();
      lib.splash.importLocalCSVFile('csv imported', importPath);
    },
    function() {
      lib.splash.expectNumProjects(0);
      lib.splash.expectNumDirectories(0);
      lib.splash.expectNumTables(1);
      lib.splash.expectTableListed('csv imported');
    }
  );
  fw.transitionTest(
    'CSV file imported as table',
    'Project vertices imported from a table',
    function() {
      lib.splash.openNewProject('csv imported project');
      lib.left.runOperation('Import vertices', {table: 'csv imported'});
    },
    function() {
      expect(lib.left.vertexCount()).toEqual(3);
      expect(lib.left.attributeCount()).toEqual(3);
    }
  );
};
