'use strict';

// Viewer of a plot state.

angular.module('biggraph')
  .directive('visualizationPopup', function(util, side) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/visualization-popup.html',
      scope: {
        projectStateId: '=',
        uiState: '=',
        onUiStateChanged: '&',
      },
      link: function(scope) {

        scope.$watch('projectStateId', function(newValue, oldValue, scope) {
          scope.sides = [];
          scope.left = new side.Side(scope.sides, 'left', scope.projectStateId, true);
          scope.right = new side.Side(scope.sides, 'right', scope.projectStateId, true);
          scope.sides.push(scope.left);
          scope.sides.push(scope.right);

          scope.sides[0].state.projectPath = '';

          scope.applyVisualizationData();
          scope.sides[0].reload();
          scope.sides[1].reload();

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
