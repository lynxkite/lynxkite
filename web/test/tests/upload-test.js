'use strict';


module.exports = function(fw) {
  var lib = require('../test-lib.js');
  var path = require('path');

  fw.transitionTest(
    'empty test-example workspace',
    'example graph vertex set names imported as table',
    function() {
      lib.workspace.addBox({
        id: 'ib0',
        name: 'Import CSV',
        x: 100, y: 100 });
      var boxEditor = lib.workspace.openBoxEditor('ib0');
      var importPath = path.resolve(__dirname, 'data/upload_test.csv');
      boxEditor.populateOperation({
        'filename': importPath
      });
      lib.loadImportedTable();
      boxEditor.close();
    },
    function() {
      var tableState = lib.workspace.openStateView('ib0', 'table');
      var table = tableState.table;
      table.expectRowCountIs(4);
      table.expectColumnNamesAre(['name']);
      table.expectColumnTypesAre(['String']);
      table.expectRowsAre([['Adam'], ['Eve'], ['Bob'], ['Isolated Joe']]);
    }
  );
};
