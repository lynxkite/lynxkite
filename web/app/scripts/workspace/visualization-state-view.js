// Viewer of a visualization state.

'use strict';
import '../app';
import '../util/util';
import '../project/side';
import templateUrl from './visualization-state-view.html?url';

angular.module('biggraph')
  .directive('visualizationStateView', function(util, side, $q) {
    return {
      restrict: 'E',
      templateUrl,
      scope: {
        stateId: '=',
        popupModel: '=',
        editHandler: '=',
      },
      link: function(scope) {
        scope.sides = [];
        scope.left = new side.Side(scope.sides, 'left');
        scope.right = new side.Side(scope.sides, 'right');
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
              scope.changed = false;
              scope.applyVisualizationData();
            }, function() {}
          );
        });

        scope.applyVisualizationData = function() {
          if (scope.visualization.$resolved) {
            const state = scope.visualization;
            scope.left.updateFromBackendJson(state.left);
            scope.right.updateFromBackendJson(state.right);
            const leftPromise = scope.left.reload();
            const rightPromise = scope.right.reload();
            const pendingReloads = [];
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
          const left = scope.left;
          const right = scope.right;
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
          const left = scope.left;
          const right = scope.right;
          if (!left.loaded() || !right.loaded()) { return undefined; }
          // If it is the same project on both sides, use its internal edges.
          if (left.project.name === right.project.name) {
            return left.project.edgeBundle;
          }
          return undefined;
        }

        util.deepWatch(
          scope,
          '[left.state, right.state]',
          function(newValue, oldValue) {
            if (oldValue === newValue ||
              !scope.changed &&
              angular.equals(scope.left.state, scope.visualization.left) &&
              angular.equals(scope.right.state, scope.visualization.right)) {
              // Initial call or application of the backend response.
              return;
            }
            scope.left.updateViewData();
            scope.right.updateViewData();
            scope.changed = true;
            if (scope.editHandler.onEdit) {
              scope.editHandler.onEdit(scope.stateJSON());
            }
          });

        scope.stateJSON = function() {
          return JSON.stringify({ left: scope.left.state, right: scope.right.state });
        };
      },
    };
  });
