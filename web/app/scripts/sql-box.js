// Presents the parameters for running SQL scripts.
'use strict';

angular.module('biggraph').directive('sqlBox', function(side, util) {
  return {
    restrict: 'E',
    scope: { side: '=' },
    templateUrl: 'sql-box.html',
    link: function(scope) {
      scope.runSQLQuery = function() {
        scope.result = util.nocache(
          '/ajax/runSQLQuery',
          {
            project: scope.side.state.projectName,
            sql: scope.sql,
            rownum: 10,
          });
      };

      scope.reportSQLError = function() {
        util.reportRequestError(scope.result, 'Error executing query.');
      };
    }
  };
});
