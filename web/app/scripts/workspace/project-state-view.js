'use strict';

// Viewer of a project state.
// This is like the project view of the old LynxKite UI.
//
// The current implementation is just a subset copy of side.js and many features are
// missing.

angular.module('biggraph')
 .directive('projectStateView', function(util, side) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/project-state-view.html',
      scope: {
        stateId: '=',
      },
      link: function(scope) {
        scope.sides = [];

        util.deepWatch(scope, 'stateId', function() {
          scope.sides = [];
          scope.left = new side.Side(scope.sides, 'left', scope.stateId);
          scope.right = new side.Side(scope.sides, 'right', scope.stateId);
          scope.sides.push(scope.left);
          scope.sides.push(scope.right);

          scope.sides[0].state.projectPath = '';
          scope.sides[0].reload();
        });

        scope.$watch(
          'left.project.$resolved',
          function(loaded) {
            if (loaded) {
              scope.left.onProjectLoaded();
            }
          });
        scope.$watch(
          'right.project.$resolved',
          function(loaded) {
            if (loaded) {
              scope.right.onProjectLoaded();
            }
          });
      },
    };
});
