// UI for importing external data.
'use strict';

angular.module('biggraph').directive('importWizard', function(util) {
  return {
    scope: { tableImported: '=', currentDirectory: '=', onCancel: '&' },
    templateUrl: 'import-wizard.html',
    link: function(scope) {
      scope.csv = {
        delimiter: ',',
        mode: 'FAILFAST',
        fileUploadCount: 0,
      };
      scope.cancel = function(event) {
        event.stopPropagation();
        scope.onCancel();
      };

      function importStuff(endpoint, parameters) {
        scope.inputsDisabled = true;
        util.post(endpoint, parameters).catch(function() {
          scope.inputsDisabled = false;
        }).then(function(result) {
          scope.tableImported = result;
        });
      }

      scope.importCSV = function() {
        importStuff(
          '/ajax/importCSV',
          {
            table: scope.tableName,
            privacy: 'public-read',
            files: scope.csv.filename,
            columnNames: scope.csv.columnNames ? scope.csv.columnNames.split(',') : [],
            delimiter: scope.csv.delimiter,
            mode: scope.csv.mode,
          });
      };
      scope.importJdbc = function() {
        var columnsToImport =
          scope.jdbc.columnsToImport ? scope.jdbc.columnsToImport.split(',') : [];
        importStuff(
          '/ajax/importJdbc',
          {
            table: scope.tableName,
            privacy: 'public-read',
            jdbcUrl: scope.jdbc.url,
            jdbcTable: scope.jdbc.table,
            keyColumn: scope.jdbc.keyColumn,
            columnsToImport: columnsToImport,
          });
      };
    },
  };
});
