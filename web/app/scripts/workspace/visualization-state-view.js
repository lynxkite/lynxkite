// Viewer of a visualization state.

'use strict';

angular.module('biggraph')
  .directive('visualizationStateView', function(util, side, $q) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/visualization-state-view.html',
      scope: {
        stateId: '=',
        popupModel: '=',
      },
      link: function(scope) {
        scope.sides = [];
        scope.left = new side.Side(scope.sides, 'left', undefined);
        scope.right = new side.Side(scope.sides, 'right', undefined);
        scope.sides.push(scope.left);
        scope.sides.push(scope.right);

        scope.$watch('stateId', function(newValue, oldValue, scope) {
          scope.left.stateId = scope.stateId;
          scope.right.stateId = scope.stateId;

          scope.visualization = util.get('/ajax/getVisualizationOutput', {
            id: scope.stateId
          });
          scope.visualization.then(
            function() {
              scope.applyVisualizationData();
            }, function() {}
          );
        });

        scope.applyVisualizationData = function() {
          if (scope.visualization.$resolved) {
            var state = scope.visualization;
            scope.left.updateFromBackendJson(state.left);
            scope.right.updateFromBackendJson(state.right);
            var leftPromise = scope.left.reload();
            var rightPromise = scope.right.reload();
            var pendingReloads = [];
            // Collect project load promises into a list and handle side completion events:
            if (leftPromise) {
              pendingReloads.push(leftPromise);
              leftPromise.then(function() {
                scope.left.onProjectLoaded();
              });
            }
            if (rightPromise) {
              pendingReloads.push(rightPromise);
              rightPromise.then(function() {
                scope.right.onProjectLoaded();
              });
            }
            // When all reloads are completed:
            $q.all(pendingReloads).then(function() {
              scope.leftToRightBundle = getLeftToRightBundle();
              scope.rightToLeftBundle = getRightToLeftBundle();
            });
          }
        };

        function getLeftToRightBundle() {
          var left = scope.left;
          var right = scope.right;
          if (!left.loaded() || !right.loaded()) { return undefined; }
          // If it is a segmentation, use "belongsTo" as the connecting path.
          if (right.isSegmentationOf(left)) {
            return left.getBelongsTo(right);
          }
          // If it is the same project on both sides, use its internal edges.
          if (left.project.name === right.project.name) {
            return left.project.edgeBundle;
          }
          return undefined;
        }

        function getRightToLeftBundle() {
          var left = scope.left;
          var right = scope.right;
          if (!left.loaded() || !right.loaded()) { return undefined; }
          // If it is the same project on both sides, use its internal edges.
          if (left.project.name === right.project.name) {
            return left.project.edgeBundle;
          }
          return undefined;
        }
      },
    };
  });
