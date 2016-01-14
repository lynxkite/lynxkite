// Presents the parameters for running SQL scripts.
'use strict';

angular.module('biggraph').directive('sqlBox', function(side, util) {
  return {
    restrict: 'E',
    scope: { side: '=' },
    templateUrl: 'sql-box.html',
    link: function(scope) {
      scope.runSQLQuery = function() {
        if (!scope.sql) {
          scope.result = { $error: 'SQL script must be specified.' };
        } else {
          scope.inProgress = true;
          scope.result = util.nocache(
            '/ajax/runSQLQuery',
            {
              project: scope.side.state.projectName,
              sql: scope.sql,
              rownum: 10,
            });
          scope.result.finally(function() {
              scope.inProgress = false;
            });
        }
      };

      scope.reportSQLError = function() {
        util.reportRequestError(scope.result, 'Error executing query.');
      };
    }
  };
});
