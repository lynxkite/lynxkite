// Presents the parameters for running SQL scripts.
'use strict';

angular.module('biggraph').directive('sqlBox', function($window, side, util) {
  return {
    restrict: 'E',
    scope: { side: '=' },
    templateUrl: 'sql-box.html',
    link: function(scope) {
      scope.inProgress = 0;
      scope.sql = 'select * from vertices';

      scope.runSQLQuery = function() {
        if (!scope.sql) {
          scope.result = { $error: 'SQL script must be specified.' };
        } else {
          scope.inProgress += 1;
          scope.result = util.nocache(
            '/ajax/runSQLQuery',
            {
              df: {
                project: scope.side.state.projectName,
                sql: scope.sql,
              },
              maxRows: 10,
            });
          scope.result.finally(function() {
            scope.inProgress -= 1;
          });
        }
      };

      scope.$watch('exportFormat', function(exportFormat) {
        if (exportFormat === 'table') {
          scope.exportKiteTable = '';
        } else if (exportFormat === 'csv') {
          scope.exportPath = '<download>';
          scope.exportDelimiter = ',';
          scope.exportQuote = '"';
          scope.exportHeader = true;
        } else if (exportFormat === 'json') {
          scope.exportPath = '<download>';
        } else if (exportFormat === 'parquet') {
          scope.exportPath = '';
        } else if (exportFormat === 'orc') {
          scope.exportPath = '';
        } else if (exportFormat === 'jdbc') {
          scope.exportJdbcUrl = '';
          scope.exportJdbcTable = '';
          scope.exportMode = 'error';
        }
      });

      scope.export = function() {
        if (!scope.sql) {
          scope.result = { $error: 'SQL script must be specified.' };
        } else {
          var req = {
            df: {
              project: scope.side.state.projectName,
              sql: scope.sql,
            },
          };
          scope.inProgress += 1;
          if (scope.exportFormat === 'table') {
            req.table = scope.exportKiteTable;
            req.privacy = 'public-read';
            scope.result = util.post('/ajax/exportSQLQueryToTable', req);
          } else if (scope.exportFormat === 'csv') {
            req.path = scope.exportPath;
            req.delimiter = scope.exportDelimiter;
            req.quote = scope.exportQuote;
            req.header = scope.exportHeader;
            scope.result = util.post('/ajax/exportSQLQueryToCSV', req);
          } else if (scope.exportFormat === 'json') {
            req.path = scope.exportPath;
            scope.result = util.post('/ajax/exportSQLQueryToJson', req);
          } else if (scope.exportFormat === 'parquet') {
            req.path = scope.exportPath;
            scope.result = util.post('/ajax/exportSQLQueryToParquet', req);
          } else if (scope.exportFormat === 'orc') {
            req.path = scope.exportPath;
            scope.result = util.post('/ajax/exportSQLQueryToORC', req);
          } else if (scope.exportFormat === 'jdbc') {
            req.jdbcUrl = scope.exportJdbcUrl;
            req.table = scope.exportJdbcTable;
            req.mode = scope.exportMode;
            scope.result = util.post('/ajax/exportSQLQueryToJdbc', req);
          } else {
            throw new Error('Unexpected export format: ' + scope.exportFormat);
          }
          scope.result.finally(function() {
            scope.inProgress -= 1;
          });
          scope.result.then(function(result) {
            scope.showExportOptions = false;
            scope.result.success = 'Results exported.';
            if (result.download) {
              var path = encodeURIComponent(result.download);
              var name = encodeURIComponent(result.download.split('/').slice(-1)[0]);
              // Fire off the download.
              $window.location = '/download?path=' + path + '&name=' + name;
            }
          });
        }
      };

      scope.reportSQLError = function() {
        util.reportRequestError(scope.result, 'Error executing query.');
      };
    }
  };
});
