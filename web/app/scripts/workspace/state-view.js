'use strict';

// Viewer of a state at an output of a box.

angular.module('biggraph')
 .directive('stateView', function(side, util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/state-view.html',
      scope: {
        state: '='
      },
      link: function(scope) {
        scope.sides = [];

        util.deepWatch(scope, 'state', function() {
          scope.sides = [];
          scope.left = new side.Side(scope.sides, 'left');
          scope.right = new side.Side(scope.sides, 'right');
          scope.sides.push(scope.left);
          scope.sides.push(scope.right);
          scope.sides[0].state.projectRef = Object.assign({}, scope.state);
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
              console.log('RIGHT LOADED');
              scope.right.onProjectLoaded();
            }
          });

        util.deepWatch(
          scope,
          'left.state.projectRef',
          function() {
            scope.left.reload();
          });
        util.deepWatch(
          scope,
          'right.state.projectRef',
          function() {
            console.log('RELOAD RIGHT');
            scope.right.reload();
          });


/*
        util.deepWatch(scope, 'state1', function() {
          if (!scope.state1) {
            return;
          }
          scope.state = util.nocache(
              '/ajax/getOutput',
              {
                  workspace: scope.state1.workspaceName,
                  output: {
                    boxID: scope.state1.boxID,
                    id: scope.state1.outputID
                  }
              });
        });
*/

/*
        scope.$watch('state.$resolved', function() {
          if (scope.state && scope.state.$resolved &&
              scope.state.kind === 'project') {
            scope.sides = [];
            scope.sides.push(new side.Side(scope.sides, 'left'));
            scope.sides.push(new side.Side(scope.sides, 'right'));
            var left = scope.sides[0];
            left.project = scope.state.project;
            left.project.$resolved = true;
            left.onProjectLoaded();
          } else {
            scope.sides = [];
          }
        });
      }
*/
    }
  };
});
