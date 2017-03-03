'use strict';

angular.module('biggraph')
 .directive('stateView', function(side) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/boxes-gui/state-view.html',
      scope: {
        state: '='
      },
      link: function(scope) {
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
