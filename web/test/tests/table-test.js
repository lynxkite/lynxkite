'use strict';


module.exports = function(fw) {
  var lib = require('../test-lib.js');
  var path = require('path');

  fw.transitionTest(
    'empty test-example workspace',
    'small CSV file imported as table',
    function() {
      var importPath = path.resolve(__dirname, 'data/import_csv_test.csv');
      lib.workspace.addBox({
        id: 'ib0',
        name: 'Import CSV',
        x: 100, y: 100 });
      var boxEditor = lib.workspace.openBoxEditor('ib0');
      var csvFileParameter = $('file-parameter');
      lib.uploadIntoFileParameter(csvFileParameter, importPath);
      boxEditor.populateOperation({
        'columns': 'name,age'
      });
      var loadButton = $('#imported_table button');
      loadButton.click();
      boxEditor.close();
    },
    function() {
    }
  );

  fw.transitionTest(
    'small CSV file imported as table',
    'table state view opened',
    function() {
      var tableState = lib.workspace.openStateView('ib0', 'table');
      var table = tableState.table;
      table.expectRowCountIs(3);
      table.expectColumnNamesAre(['name', 'age']);
      table.expectColumnTypesAre(['String', 'String']);
      table.expectRowsAre([['Adam', '24'], ['Eve', '32'], ['Bob', '41']]);
    },
    function() {
    }
  );

  fw.transitionTest(
    'empty test-example workspace',
    'large CSV file imported as table',
    function() {
      var importPath = path.resolve(__dirname, 'data/import_large_csv_test.csv');
      lib.workspace.addBox({
        id: 'ib1',
        name: 'Import CSV',
        x: 400, y: 100 });
      var boxEditor = lib.workspace.openBoxEditor('ib1');
      var csvFileParameter = $('file-parameter');
      lib.uploadIntoFileParameter(csvFileParameter, importPath);
      boxEditor.populateOperation({
        'infer': 'yes'
      });
      var loadButton = $('#imported_table button');
      loadButton.click();
      boxEditor.close();
    },
    function() {
    }
  );

  fw.statePreservingTest(
    'large CSV file imported as table',
    'Sorting and Show more rows are working on table state view',
    function() {
      var state = lib.workspace.openStateView('ib1', 'table');
      var table = state.table;
      table.expectRowCountIs(10);
      table.expectColumnNamesAre([
        'country', 'country_code', 'population', 'area', 'currency']);
      table.expectColumnTypesAre([
        'String', 'String', 'Int', 'Int', 'String']);
      table.expectFirstRowIs([
        'Afghanistan', 'AF', '33332025', '652230', 'AFN']);
      table.clickColumn(2); // population column, asc
      table.expectFirstRowIs([
        'Antarctica', 'AQ', '0', '14000000', 'null']);
      table.clickColumn(0); // country column, desc
      table.clickColumn(0);
      table.expectFirstRowIs([
        'Åland Islands', 'AX', '29013', '1580', 'EUR']);
      table.clickShowMoreRows();
      table.expectRowCountIs(20);
      table.clickShowMoreRows();
      table.expectRowCountIs(30);
      table.setRowCount(7);
      table.clickShowSample();
      table.expectRowCountIs(7);
    }
  );



/*
  fw.transitionTest(
    'CSV file imported as table',
    'Table described in global SQL box',
    function() {
      lib.splash.table(tableName).click();
    },
    function() {
      lib.splash.expectGlobalSqlResult(['name'], ['String'], [['Adam'], ['Eve'], ['Bob']]);
    }
  );

  fw.transitionTest(
    'Table described in global SQL box',
    'CSV file imported as table with limit',
    function() {
      var importPath = path.resolve(__dirname, 'data/import_csv_test.csv');
      lib.splash.startTableImport();
      lib.splash.importLocalCSVFile(tableName, importPath, 'name,age', 'name', false, '2');
      lib.confirmSweetAlert('Entry already exists');
      lib.splash.computeTable(tableName);
    },
    function() {
      lib.splash.expectNumProjects(0);
      lib.splash.expectNumDirectories(0);
      lib.splash.expectNumTables(1);
      lib.splash.expectTableWithNumRows(tableName, 2);
    }
  );

  fw.transitionTest(
    'CSV file imported as table with limit',
    'Table edited to remove limit',
    function() {
      lib.splash.editTable(tableName);
      expect(lib.splash.root.$('import-wizard #limit input').getAttribute('value')).toBe('2');
      lib.splash.root.$('import-wizard #limit input').clear();
      lib.splash.clickAndWaitForCsvImport();
      lib.splash.computeTable(tableName);
    },
    function() {
      lib.splash.expectNumProjects(0);
      lib.splash.expectNumDirectories(0);
      lib.splash.expectNumTables(1);
      lib.splash.expectTableWithNumRows(tableName, 3);
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
      expect(lib.left.attributeCount()).toEqual(2);  // id,name
    }
  );

  fw.statePreservingTest(
    'CSV file imported as table',
    'Editing imported CSV configuration is possible',
    function() {
      lib.splash.editTable('csv imported');
      expect(element(by.css('#table-name input')).getAttribute('value')).toEqual(tableName);
      expect(element(by.css('#csv-column-names input')).getAttribute('value')).toEqual('name,age');
      expect(element(by.css('#columns-to-import input')).getAttribute('value')).toEqual('name');

      lib.splash.clickAndWaitForCsvImport();
      lib.splash.computeTable(tableName);
      expect(lib.errors()).toEqual([]);
      lib.splash.expectNumProjects(0);
      lib.splash.expectNumDirectories(0);
      lib.splash.expectNumTables(1);
      lib.splash.expectTableListed(tableName);
      lib.splash.expectTableWithNumRows(tableName, 3);
    }
  );

  fw.transitionTest(
    'empty splash',
    'CSV file imported as table with header',
    function() {
      var importPath = path.resolve(__dirname, 'data/import_csv_test_hdr.csv');
      lib.splash.startTableImport();
      lib.splash.importLocalCSVFile(tableName, importPath, '', '');
      // Check if table was created:
      lib.splash.expectNumProjects(0);
      lib.splash.expectNumDirectories(0);
      lib.splash.expectNumTables(1);
      lib.splash.computeTable(tableName);
      lib.splash.expectTableWithNumRows(tableName, 3);
      // Import into project:
      lib.splash.openNewProject('csv imported project2');
      lib.left.runOperation('Import vertices', {table: tableName});
    },
    function() {
      expect(lib.left.vertexCount()).toEqual(3);
      expect(lib.left.attributeCount()).toEqual(3);  // id,name,age
    }
  );

  var jdbcImportPath = path.resolve(__dirname, 'data/import_jdbc_test.sqlite');
  var jdbcImportPath2 = path.resolve(__dirname, 'data/import_jdbc_test2.sqlite');
  var jdbcUrl = 'jdbc:sqlite:' + jdbcImportPath;
  var jdbcUrl2 = 'jdbc:sqlite:' + jdbcImportPath2;

  fw.transitionTest(
    'empty splash',
    'Sqlite file imported via JDBC as table',
    function() {
      lib.splash.startTableImport();
      lib.splash.importJDBC('jdbc imported', jdbcUrl, 'table1', 'a');
      lib.splash.computeTable('jdbc imported');
      expect(lib.errors()).toEqual([]);
    },
    function() {
      lib.splash.expectNumProjects(0);
      lib.splash.expectNumDirectories(0);
      lib.splash.expectNumTables(1);
      lib.splash.expectTableWithNumRows('jdbc imported', 3);
    });

  fw.transitionTest(
    'Sqlite file imported via JDBC as table',
    'Editing imported JDBC configuration is possible',
    function() {
      lib.splash.editTable('jdbc imported');
      expect(element(by.model('tableName')).getAttribute('value')).toEqual('jdbc imported');
      var jdbcInput = element(by.css('#jdbc-url input'));
      expect(jdbcInput.getAttribute('value')).toEqual(jdbcUrl);
      jdbcInput.sendKeys(lib.selectAllKey + jdbcUrl2);
      element(by.id('import-jdbc-button')).click();
      lib.splash.computeTable('jdbc imported');
      expect(lib.errors()).toEqual([]);
    },
    function() {
      lib.splash.expectNumProjects(0);
      lib.splash.expectNumDirectories(0);
      lib.splash.expectNumTables(1);
      lib.splash.expectTableWithNumRows('jdbc imported', 4);
    });
*/
};

