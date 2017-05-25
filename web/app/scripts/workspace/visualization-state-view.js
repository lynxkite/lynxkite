'use strict';

// Viewer of a plot state.

angular.module('biggraph')
  .directive('visualizationStateView', function(util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/visualization-state-view.html',
      scope: {
        stateId: '=',
      },
      link: function(scope) {

        scope.$watch('stateId', function(newValue, oldValue, scope) {
          scope.visualization = util.get('/ajax/getVisualizationOutput', {
            id: scope.stateId
          });
          scope.visualization.then(
            function() {
              console.log(scope.visualization);
            }, function() {}
          );
        });


      },
    };
  });
