'use strict';

// Viewer of a plot state.

angular.module('biggraph')
  .directive('plotStateView', function(util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/plot-state-view.html',
      scope: {
        stateId: '=',
      },
      link: function(scope) {
        scope.plot = 'placeholder for plot description';

        scope.getSample = function() {
          scope.table = util.get('/ajax/getTableOutput', {
            id: scope.stateId,
            sampleRows: scope.sampleRows,
          });
        };

      },
    };
  });
