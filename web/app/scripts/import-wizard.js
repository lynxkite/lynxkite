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
      scope.files = {
        fileUploadCount: 0,
      };
      scope.cancel = function(event) {
        event.stopPropagation();
        scope.onCancel();
      };

      scope.requestInProgress = 0;
      function importStuff(endpoint, parameters) {
        parameters.table = scope.tableName;
        parameters.privacy = 'public-read';
        scope.requestInProgress += 1;
        var request = util.post(endpoint, parameters);
        request.then(function(result) {
          scope.tableImported = result;
        });
        request.finally(function() {
          scope.requestInProgress -= 1;
        });
      }

      function splitCSVLine(csv) {
        return csv ? csv.split(',') : [];
      }

      scope.importCSV = function() {
        importStuff(
          '/ajax/importCSV',
          {
            files: scope.csv.filename,
            columnNames: splitCSVLine(scope.csv.columnNames),
            delimiter: scope.csv.delimiter,
            mode: scope.csv.mode,
          });
      };

      function importFilesWith(request) {
        importStuff(
          request,
          {
            files: scope.files.filename,
            columnsToImport: splitCSVLine(scope.files.columnsToImport),
          });
      }

      scope.importParquet = function() {
        importFilesWith('/ajax/importParquet');
      };
      scope.importORC = function() {
        importFilesWith('/ajax/importORC');
      };
      scope.importJdbc = function() {
        var columnsToImport =
          splitCSVLine(scope.jdbc.columnsToImport);
        importStuff(
          '/ajax/importJdbc',
          {
            jdbcUrl: scope.jdbc.url,
            jdbcTable: scope.jdbc.table,
            keyColumn: scope.jdbc.keyColumn,
            columnsToImport: columnsToImport,
          });
      };
    },
  };
});
