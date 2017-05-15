'use strict';

// Viewer of a state at an output of a box.

angular.module('biggraph')
  .directive('stateView', function(util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/state-view.html',
      scope: {
        workspace: '=',
        boxId: '=',
        plugId: '=',
      },
      link: function(scope) {
        util.deepWatch(
            scope,
            function() {
              if (scope.boxId && scope.plugId && scope.workspace) {
                var plug = scope.workspace.getOutputPlug(
                    scope.boxId, scope.plugId);
                return {stateId: plug.stateId, kind: plug.kind};
              } else {
                return undefined;
              }
            },
            function(plugDesc) {
              if (plugDesc) {
                scope.stateId = plugDesc.stateId;
                scope.stateKind = plugDesc.kind;
              }
            });

        scope.createSnapshot = function(saveAsName, success, error) {
          util.post('/ajax/createSnapshot', {
            name: saveAsName,
            id: scope.stateId
          }).then(success, error);
        };
      },
    };
  });
