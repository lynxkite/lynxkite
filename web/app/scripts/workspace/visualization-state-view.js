'use strict';

// Viewer of a plot state.

angular.module('biggraph')
  .directive('visualizationStateView', function(util, side) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/visualization-state-view.html',
      scope: {
        stateId: '=',
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
              console.log('LOAD');
              console.log(scope.visualization);
              console.log(scope.visualization.status);
              scope.left.state = scope.visualization.status;
            }, function() {}
          );
        });

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
          function(leftLoaded, rightLoaded) {
            if (leftLoaded) {
              scope.left.onProjectLoaded();
            }
            if (rightLoaded) {
              scope.right.onProjectLoaded();
            }
            if (leftLoaded || rightLoaded) {
              scope.leftToRightBundle = getLeftToRightBundle();
              scope.rightToLeftBundle = getRightToLeftBundle();
            }

          });

      },
    };
  });
