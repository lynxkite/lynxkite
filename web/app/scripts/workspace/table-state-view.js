'use strict';

// Viewer of a table state.
// This is like the SQL result box, just shows the schema
// of the table and the first few rows.

angular.module('biggraph')
 .directive('tableStateView', function(util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/table-state-view.html',
      scope: {
        stateId: '=',
      },
      link: function(scope) {
        scope.table = null;
        scope.$watch('stateId', function() {
          scope.table = util.get('/ajax/getTableOutput', {
            id: scope.stateId,
          });
        });
      },
    };
});
