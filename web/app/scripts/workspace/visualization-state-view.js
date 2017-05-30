'use strict';

// Viewer of a plot state.

angular.module('biggraph')
  .directive('visualizationStateView', function(util, side) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/visualization-state-view.html',
      scope: {
        stateId: '=',
        popupModel: '=',
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
            scope.left.state = scope.visualization.status;
            scope.left.updateViewData();
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
              console.log('LOAD LEFT');
              scope.left.onProjectLoaded();
              console.log('LOAD LEFT: DONE');
            }
            if (rightLoaded) {
              console.log('right resolved? ', rightLoaded);
              console.log('right resolved? ', scope.right.project.$resolved);
              console.log(scope.right.project);
              console.log('LOAD RIGHT');
              scope.right.onProjectLoaded();
              console.log('LOAD RIGHT: DONE');
            }
            if (leftLoaded || rightLoaded) {
              scope.leftToRightBundle = getLeftToRightBundle();
              scope.rightToLeftBundle = getRightToLeftBundle();
              scope.applyVisualizationData();
            }

          });

      },
    };
  });
