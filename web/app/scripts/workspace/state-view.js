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
          console.log('lalalalalalla lallala                    alalalalalal');
          scope.createSnapshot = function(saveAsName) {
            scope.saving = true;
            console.log('snapshotting');
            util.post('/ajax/createSnapshot', {
              name: saveAsName,
              id: scope.workspace.selectedStateId
            }).finally(function() {
              console.log(scope.workspace);
              console.log(scope.workspace.selectedStateId);
              console.log('goddamnfina;lly');
              scope.saving = false;
            });
          };
          window.ss = scope.createSnapshot;
        });
      },
    };
});
