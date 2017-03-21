'use strict';

// Viewer of a state at an output of a box.

angular.module('biggraph')
 .directive('stateView', function(side, util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/state-view.html',
      scope: {
        state1: '=state'
      },
      link: function(scope) {
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

        scope.$watch('state.$resolved', function() {
          if (scope.state && scope.state.$resolved &&
              scope.state.kind === 'project') {
            scope.side = new side.Side([], '');
            scope.side.project = scope.state.project;
            scope.side.project.$resolved = true;
            scope.side.onProjectLoaded();
          } else {
            scope.side = undefined;
          }
        });
      }
    };
});
