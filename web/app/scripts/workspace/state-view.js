'use strict';

// Viewer of a state at an output of a box.

angular.module('biggraph')
 .directive('stateView', function(util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/state-view.html',
      scope: {
        workspace: '=',
        stateId: '=',
        stateKind: '=',
      },
      link: function(scope) {

        scope.createSnapshot = function(saveAsName) {
          scope.saving = true;
          util.post('/ajax/createSnapshot', {
            name: saveAsName,
            id: scope.stateId
          }).finally(function() {
            scope.saving = false;
          });
        };
      },
    };
});
