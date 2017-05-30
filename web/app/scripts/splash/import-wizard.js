// UI for importing external data.
'use strict';

angular.module('biggraph').directive('importWizard', function($q, util) {
  return {
    scope: { tableImported: '=', currentDirectory: '=', onCancel: '&' },
    templateUrl: 'scripts/splash/import-wizard.html',
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
      scope.limit = '';

      scope.requestInProgress = 0;
      function importStuff(format, parameters, options) {
        options = options || { overwrite: false };
        parameters.table =
          (scope.currentDirectory ? scope.currentDirectory + '/' : '') + scope.tableName;
        parameters.privacy = 'public-read';
        parameters.columnsToImport = splitCSVLine(scope.columnsToImport);
        parameters.asView = scope.asView;
        if (scope.limit) {
          parameters.limit = parseInt(scope.limit);
        } else {
          parameters.limit = null;
        }

        // Allow overwriting the same name when editing an existing config.
        parameters.overwrite = options.overwrite || scope.oldTableName === scope.tableName;
        scope.requestInProgress += 1;

        var importOrView = scope.asView ? 'createView' : 'import';
        var endpoint = '/ajax/' + importOrView + format;
        var request = util.post(endpoint, parameters, { reportErrors: false });

        request.catch(function importErrorHandler(error) {
          if (error.data === 'file-already-exists-confirm-overwrite') {
            util.showOverwriteDialog(function() {
              importStuff(format, parameters, { overwrite: true });
            });
          } else {
            util.ajaxError(error);
          }
          return $q.reject(error);
        }).then(function importDoneHandler(result) {
          scope.tableImported = result;
        }).finally(function() {
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
          'CSV',
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

      scope.$on('fill import from config', function(evt, newConfig, tableName, type) {
        scope.tableName = tableName;
        scope.oldTableName = tableName;
        scope.columnsToImport = joinCSVLine(newConfig.data.columnsToImport);
        scope.asView = type === 'view';
        scope.limit = newConfig.data.limit;

        // E.g.: "com.lynxanalytics.biggraph.controllers.CSVImportRequest"
        // becomes "CSVImportRequest".
        var requestName = newConfig.class.split('.').pop();
        var suffixLength = 'ImportRequest'.length;
        var datatype = requestName.slice(0, -suffixLength).toLowerCase();
        scope.datatype = datatype;

        if (datatype === 'jdbc') {
          scope.jdbc = {};
          fillJdbcFromData(scope.jdbc, newConfig.data);
        } else if (datatype === 'hive') {
          scope.hive = {};
          fillHiveFromData(scope.hive, newConfig.data);
        } else if (datatype === 'csv') {
          scope.csv = {};
          fillCSVFromData(scope.csv, newConfig.data);
        } else {
          scope.files = {};
          fillScopeFromData(scope.files, newConfig.data);
        }
      });

      function fillScopeFromData(datatypeScope, data) {
        if (data.files) {
          datatypeScope.filename = data.files;
        }
        for (var newConfigItem in data) {
          if (newConfigItem in data) {
            datatypeScope[newConfigItem] = data[newConfigItem];
          }
        }
      }

      function fillCSVFromData(csv, data) {
        csv.filename = data.files;
        csv.columnNames = joinCSVLine(data.columnNames);
        csv.delimiter = data.delimiter;
        csv.mode = data.mode;
        csv.infer = data.infer;
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
        importFilesWith('Parquet');
      };
      scope.importORC = function() {
        importFilesWith('ORC');
      };
      scope.importJson = function() {
        importFilesWith('Json');
      };
      scope.importJdbc = function() {
        importStuff(
          'Jdbc',
          {
            jdbcUrl: scope.jdbc.url,
            jdbcTable: scope.jdbc.table,
            keyColumn: scope.jdbc.keyColumn,
          });
      };
      scope.importHive = function() {
        importStuff(
          'Hive', {
            hiveTable: scope.hive.tableName,
          });
      };
    },
  };
});
