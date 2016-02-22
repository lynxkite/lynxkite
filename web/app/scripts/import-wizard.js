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
        parameters.table =
          (scope.currentDirectory ? scope.currentDirectory + '/' : '') + scope.tableName;
        parameters.privacy = 'public-read';
        parameters.columnsToImport = splitCSVLine(scope.columnsToImport);
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
          });
      }

      scope.importParquet = function() {
        importFilesWith('/ajax/importParquet');
      };
      scope.importORC = function() {
        importFilesWith('/ajax/importORC');
      };
      scope.importJson = function() {
        importFilesWith('/ajax/importJson');
      };
      scope.importJdbc = function() {
        importStuff(
          '/ajax/importJdbc',
          {
            jdbcUrl: scope.jdbc.url,
            jdbcTable: scope.jdbc.table,
            keyColumn: scope.jdbc.keyColumn,
          });
      };
      scope.importHive = function() {
        importStuff(
          '/ajax/importHive', {
            hiveTable: scope.hive.tableName,
          });
      };
    },
  };
});
