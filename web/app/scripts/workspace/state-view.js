'use strict';

// Viewer of a state at an output of a box.

angular.module('biggraph')
 .directive('stateView', function(side, util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/state-view.html',
      scope: {
        state: '=',
        stateId: '='
      },
      link: function(scope) {
        scope.$watch('state.$resolved', function() {
          scope.createSnapshot = function() {
            util.post(
              '/ajax/createSnapshot',
              {
                id: scope.side.stateID
              });
          };
          if (scope.state && scope.state.$resolved &&
              scope.state.kind === 'project') {
            scope.side = new side.Side([], '');
            scope.side.project = scope.state.project;
            scope.side.stateID = scope.stateId;
            scope.side.project.$resolved = true;
            scope.side.onProjectLoaded();
          } else {
            scope.side = undefined;
          }
        });
      }
    };
});
