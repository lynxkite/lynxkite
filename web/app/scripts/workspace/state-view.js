'use strict';

// Viewer of a state at an output of a box.

angular.module('biggraph')
 .directive('stateView', function(util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/state-view.html',
      scope: {
        workspace: '='
      },
      link: function(scope) {
        scope.$watch('workspace.$resolved', function() {
          scope.createSnapshot = function(saveAsName) {
            scope.saving = true;
            util.post('/ajax/createSnapshot', {
              name: saveAsName,
              id: scope.workspace.selectedStateId
            }).finally(function() {
              scope.saving = false;
            });
          };
          window.ss = scope.createSnapshot;
        });
      },
    };
});
