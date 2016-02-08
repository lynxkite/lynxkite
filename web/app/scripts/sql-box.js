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
          util.error('SQL script must be specified.');
          return;
        }
        var req = {
          df: {
            project: scope.side.state.projectName,
            sql: scope.sql,
          },
        };
        scope.inProgress += 1;
        var result;
        if (scope.exportFormat === 'table') {
          req.table = scope.exportKiteTable;
          req.privacy = 'public-read';
          result = util.post('/ajax/exportSQLQueryToTable', req);
        } else if (scope.exportFormat === 'csv') {
          req.path = scope.exportPath;
          req.delimiter = scope.exportDelimiter;
          req.quote = scope.exportQuote;
          req.header = scope.exportHeader;
          result = util.post('/ajax/exportSQLQueryToCSV', req);
        } else if (scope.exportFormat === 'json') {
          req.path = scope.exportPath;
          result = util.post('/ajax/exportSQLQueryToJson', req);
        } else if (scope.exportFormat === 'parquet') {
          req.path = scope.exportPath;
          result = util.post('/ajax/exportSQLQueryToParquet', req);
        } else if (scope.exportFormat === 'orc') {
          req.path = scope.exportPath;
          result = util.post('/ajax/exportSQLQueryToORC', req);
        } else if (scope.exportFormat === 'jdbc') {
          req.jdbcUrl = scope.exportJdbcUrl;
          req.table = scope.exportJdbcTable;
          req.mode = scope.exportMode;
          result = util.post('/ajax/exportSQLQueryToJdbc', req);
        } else {
          throw new Error('Unexpected export format: ' + scope.exportFormat);
        }
        result.finally(function() {
          scope.inProgress -= 1;
        });
        result.then(function(result) {
          scope.showExportOptions = false;
          scope.success = 'Results exported.';
          if (result.download) {
            // Fire off the download.
            $window.location =
              '/downloadFile?q=' + encodeURIComponent(JSON.stringify(result.download));
          }
        });
      };

      scope.$watch('showExportOptions', function(showExportOptions) {
        if (showExportOptions) {
          scope.success = ''; // Hide previous success message to avoid confusion.
        }
      });

      scope.reportSQLError = function() {
        util.reportRequestError(scope.result, 'Error executing query.');
      };
    }
  };
});
