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
        infer: false,
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

      function joinCSVLine(csv) {
        return csv ? csv.join(',') : '';
      }

      scope.importCSV = function() {
        importStuff(
          '/ajax/importCSV',
          {
            files: scope.csv.filename,
            columnNames: splitCSVLine(scope.csv.columnNames),
            delimiter: scope.csv.delimiter,
            mode: scope.csv.mode,
            infer: scope.csv.infer,
          });
      };

      function importFilesWith(request) {
        importStuff(
          request,
          {
            files: scope.files.filename,
          });
      }

      scope.$on('fill import from config', function(evt, newConfig, tableName) {
        scope.tableName = tableName;

        scope.columnsToImport = joinCSVLine(newConfig.data.columnsToImport);

        //com.lynxanalytics.biggraph.controllers.CSVImportRequest
        var requestName = newConfig.class.split('.').pop(); //CSVImportRequest
        var suffixLength = 'ImportRequest'.length;
        var datatype = requestName.slice(0, -suffixLength).toLowerCase();
        scope.datatype = datatype;
        var datatypeScope = scope.$eval(datatype);

        if (newConfig.data.files) {
          datatypeScope.filename = newConfig.data.files;
        }

        if (datatypeScope === 'jdbc') {
          fillJdbcFromData(datatypeScope, newConfig.data);
        } else if (datatypeScope === 'hive') {
          fillHiveFromData(datatypeScope, newConfig.data);
        } else {
          fillScopeFromData(datatypeScope, newConfig.data);
        }
      });

      function fillScopeFromData(datatypeScope, data) {
        for (var newConfigItem in data) {
          if (newConfigItem in datatypeScope) {
            datatypeScope[newConfigItem] = data[newConfigItem];
          }
        }
      }

      function fillJdbcFromData(jdbc, data) {
        jdbc.url = data.jdbcUrl;
        jdbc.table = data.jdbcTable;
        jdbc.keyColumn = data.keyColumn;
      }

      function fillHiveFromData(hive, data) {
        hive.tableName = data.hiveTable;
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
