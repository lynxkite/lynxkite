'use strict';

// Editor for a visualization state stored in a parameter string.

angular.module('biggraph')
  .directive('visualizationParameter', function(util, side, $q) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/operation/visualization-parameter.html',
      scope: {
        projectStateId: '=',
        uiState: '=',
        onUiStateChanged: '&',
      },
      link: function(scope) {
        scope.sides = [];
        scope.left = new side.Side(scope.sides, 'left', undefined, true);
        scope.right = new side.Side(scope.sides, 'right', undefined, true);
        scope.sides.push(scope.left);
        scope.sides.push(scope.right);


        scope.$watch('projectStateId', function(newValue, oldValue, scope) {
          scope.left.stateId = scope.projectStateId;
          scope.right.stateId = scope.projectStateId;
          scope.applyVisualizationData();
          var leftPromise = scope.left.reload();
          var rightPromise = scope.right.reload();
          // Collect project load promises into a list and handle side completion events:
          var pendingReloads = [];
          if (leftPromise) {
            pendingReloads.push(leftPromise);
            leftPromise.then(function() { scope.left.onProjectLoaded(); });
          }
          if (rightPromise) {
            pendingReloads.push(rightPromise);
            rightPromise.then(function() { scope.right.onProjectLoaded(); });
          }
          // Completion trigger when all side's projects are loaded:
          $q.all(pendingReloads).then(function() {
            scope.leftToRightBundle = getLeftToRightBundle();
            scope.rightToLeftBundle = getRightToLeftBundle();
          });
        });

        scope.applyVisualizationData = function() {
          var state = {
            left: undefined,
            right: undefined,
          };
          if (scope.uiState) {
            state = JSON.parse(scope.uiState);
          }

          if (state.left) {
            scope.left.updateFromBackendJson(state.left);
          } else {
            scope.left.cleanState();
            scope.left.state.projectPath = '';
            scope.left.state.graphMode = 'sampled';
          }
          if (state.right) {
            scope.right.updateFromBackendJson(state.right);
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
/*
        scope.$watchGroup(
          ['left.project.$resolved', 'right.project.$resolved'],
          function(result) {
            var leftLoaded = result[0];
            var rightLoaded = result[1];
            if (leftLoaded) {
              scope.left.onProjectLoaded();
            }
            if (rightLoaded) {
              scope.right.onProjectLoaded();
            }
            if (leftLoaded || rightLoaded) {
              scope.leftToRightBundle = getLeftToRightBundle();
              scope.rightToLeftBundle = getRightToLeftBundle();
              // scope.applyVisualizationData();
            }

          });
*/

        scope.saveBoxState = function() {
          scope.uiState = JSON.stringify({
            left: scope.left.state,
            right: scope.right.state
          });
          scope.onUiStateChanged();
        };

        util.deepWatch(
          scope,
          '[left.state, right.state]',
          function(newVal, oldVal) {
            if (oldVal === newVal) {
              // This was the initial watch call.
              return;
            }
            scope.left.updateViewData();
            scope.right.updateViewData();
            scope.saveBoxState();
          });
      },
    };
  });
