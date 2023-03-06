'use strict';
import '../app';
import '../util/util';
import '../project/side';
import templateUrl from './project-state-view.html?url';

// Viewer of a project state.

angular.module('biggraph')
  .directive('projectStateView', ["util", "side", function(util, side) {
    return {
      restrict: 'E',
      templateUrl,
      scope: {
        stateId: '=',
      },
      link: function(scope) {
        scope.sides = [];
        scope.left = new side.Side(scope.sides, 'left');
        scope.left.state.projectPath = '';
        scope.right = new side.Side(scope.sides, 'right');
        scope.sides.push(scope.left);
        scope.sides.push(scope.right);

        scope.$watch('stateId', function() {
          scope.sides[0].stateId = scope.stateId;
          scope.sides[1].stateId = scope.stateId;
          scope.sides[0].reload();
          scope.sides[1].reload();
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
  }]);
