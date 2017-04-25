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
        scope.sampleRows = 10;

        scope.getSample = function() {
          scope.table = util.get('/ajax/getTableOutput', {
            id: scope.stateId,
            sampleRows: scope.sampleRows,
          });
        };

        scope.onload = scope.getSample();

        scope.showMoreRowsIncrement = function() {
          // Offer increases of 10, 100, 1000, etc. depending on the magnitude of the current limit.
          return Math.max(10, Math.pow(10, Math.floor(Math.log10(scope.sampleRows))));
        };

        scope.showMoreRows = function() {
          scope.sampleRows += scope.showMoreRowsIncrement();
          scope.getSample();
        };

        scope.sort = {
          column: undefined,
          reverse: false,
          select: function(index) {
            index = index.toString();
            if (scope.sort.column === index) {
              if (scope.sort.reverse) {
                // Already reversed by this column. This click turns off sorting.
                scope.sort.column = undefined;
              } else {
                // Already sorting by this column. This click reverses.
                scope.sort.reverse = true;
              }
            } else {
              // Not sorted yet. This click sorts by this column.
              scope.sort.column = index;
              scope.sort.reverse = false;
            }
          },
          style: function(index) {
            index = index.toString();
            if (index === scope.sort.column) {
              return scope.sort.reverse ? 'sort-desc' : 'sort-asc';
            }
          },
      };

      scope.sortKey = function(a) {
        var col = scope.sort.column;
        var dv = a[col];
        return dv && dv.defined && (dv.double !== undefined ? dv.double : dv.string);
        };
      },
    };
});
