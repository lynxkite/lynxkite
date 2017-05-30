'use strict';

// Viewer of a plot state.

angular.module('biggraph')
  .directive('visualizationPopup', function(util, side) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/visualization-popup.html',
      scope: {
        stateId: '=',
        popupModel: '=',
        box: '=',
      },
      link: function(scope) {

        scope.$watch('stateId', function(newValue, oldValue, scope) {
          scope.sides = [];
          scope.left = new side.Side(scope.sides, 'left', scope.stateId);
          scope.right = new side.Side(scope.sides, 'right', scope.stateId);
          scope.sides.push(scope.left);
          scope.sides.push(scope.right);

          scope.sides[0].state.projectPath = '';

          scope.sides[0].reload();

          scope.applyVisualizationData();
        });

        scope.applyVisualizationData = function() {
          scope.left.updateFromBackendJson(
              scope.box.instance.parameters.uiStatusJson);
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
              scope.applyVisualizationData();
            }

          });

        util.deepWatch(scope, 'left.state', function() {
          scope.left.updateViewData();
        });
        util.deepWatch(scope, 'right.state', function() {
          scope.right.updateViewData();
        });

      },
    };
  });
