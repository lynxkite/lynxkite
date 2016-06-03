// Presents the parameters for running SQL scripts.
'use strict';

angular.module('biggraph').directive('sqlBox', function($window, side, util) {
  return {
    restrict: 'E',
    scope: {
      side: '=?',
      directory: '=?',
      hideable: '=?'
     },
    templateUrl: 'sql-box.html',
    link: function(scope) {
      scope.inProgress = 0;
      if(!!scope.side && !!scope.directory) {
        throw 'can not be both defined: scope.side, scope.directory';
      }
      if(!scope.side && !scope.directory) {
        throw 'one of them needs to be defined: scope.side, scope.directory';
      }
      scope.isGlobal = !scope.side;
      scope.sql = scope.isGlobal ? 'select * from `directory/project|vertices`' :
       'select * from vertices';
      scope.project = scope.project = scope.side && scope.side.state.projectName;
      scope.sort = {
        column: undefined,
        reverse: false,
        select: function(index) {
          index = index.toString();
          if (scope.sort.column === index) {
            scope.sort.reverse = !scope.sort.reverse;
          }
          scope.sort.column = index;
        },
        style: function(index) {
          index = index.toString();
          if (index === scope.sort.column) {
            return scope.sort.reverse ? 'sort-desc' : 'sort-asc';
          }
        },
      };

      scope.runSQLQuery = function() {
        if (!scope.sql) {
          scope.result = { $error: 'SQL script must be specified.' };
        } else {
          scope.inProgress += 1;
          scope.result = util.nocache(
            '/ajax/runSQLQuery',
            {
              df: {
                isGlobal: scope.isGlobal,
                directory: scope.directory,
                project: scope.project,
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
        if (exportFormat === 'table' ||
            exportFormat === 'segmentation') {
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
            isGlobal: scope.isGlobal,
            directory: scope.directory,
            project: scope.project,
            sql: scope.sql,
          },
        };
        scope.inProgress += 1;
        var result;
        if (scope.exportFormat === 'table') {
          req.table = scope.exportKiteTable;
          req.privacy = 'public-read';
          result = util.post('/ajax/exportSQLQueryToTable', req);
        } else if (scope.exportFormat === 'segmentation') {
          result = scope.side.applyOp(
              'Create-segmentation-from-SQL',
              {
                name: scope.exportKiteTable,
                sql: scope.sql
              });
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
          if (result && result.download) {
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
