// Presents the parameters for running SQL scripts.
'use strict';

angular.module('biggraph').directive('sqlBox', function($window, side, util) {
  return {
    restrict: 'E',
    scope: { side: '=' },
    templateUrl: 'sql-box.html',
    link: function(scope) {
      scope.inProgress = 0;
      scope.sql = 'select * from `!vertices`';

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

      scope.openExportOptions = function() {
        scope.showExportOptions = true;
        scope.exportFormat = 'csv';
        scope.exportPath = '<download>';
      };

      scope.export = function() {
        if (!scope.sql) {
          scope.result = { $error: 'SQL script must be specified.' };
        } else {
          scope.inProgress += 1;
          scope.result = util.post(
            '/ajax/exportSQLQuery',
            {
              df: {
                project: scope.side.state.projectName,
                sql: scope.sql,
              },
              format: scope.exportFormat,
              path: scope.exportPath,
              options: {},
            });
          scope.result.finally(function() {
            scope.inProgress -= 1;
          });
          scope.result.then(function(result) {
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
