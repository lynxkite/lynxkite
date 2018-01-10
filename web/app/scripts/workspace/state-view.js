'use strict';

// Viewer of a state at an output of a box.

angular.module('biggraph')
  .directive('stateView', function(util, $timeout) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/state-view.html',
      scope: {
        workspace: '=',
        plug: '=',
        popupModel: '=',
      },
      link: function(scope) {
        scope.instruments = [];
        scope.$watch('plug.stateId', update);
        var lastJson;

        function update() {
          if (scope.instruments.length > 0) {
            var query = {
              workspace: scope.workspace.ref(),
              inputStateId: scope.plug.stateId,
              instruments: scope.instruments };
            var json = JSON.stringify(query);
            if (json !== lastJson) {
              scope.result = util.get('/ajax/getInstrumentedState', query);
              var currentRequest = scope.result;
              currentRequest.then(function(res) {
                if (scope.result === currentRequest) { // It is not an abandoned request.
                  scope.lastState = res.states[res.states.length - 1];
                }
              });
              lastJson = json;
            }
          } else {
            scope.result = { states: [scope.plug], metas: [] };
            scope.lastState = scope.plug;
          }
        }
        scope.onBlur = function() {
          $timeout(update); // Allow for changes to propagate to local scope.
        };

        scope.getDefaultSnapshotName = function() {
          return scope.workspace.name + '-' + scope.plugId;
        };

        scope.setInstrument = function(index, operationId) {
          scope.instruments.splice(index);
          scope.instruments.push({
            operationId: operationId,
            parameters: {},
            parametricParameters: {},
          });
          update();
        };

        scope.clearInstrument = function(index) {
          scope.instruments.splice(index);
          update();
        };

        scope.createSnapshot = function(stateId, saveAsName, success, error) {
          var postOpts = { reportErrors: false };
          util.post('/ajax/createSnapshot', {
            name: saveAsName,
            id: stateId,
          }, postOpts).then(success, error);
        };
      },
    };
  });
