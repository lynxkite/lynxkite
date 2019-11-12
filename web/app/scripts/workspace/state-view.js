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
        scope.root = { snapshotNameOpen: false }; // Dealing with ng-if scopes.
        scope.$watch('plug.stateId', update);
        let lastJson;

        function update() {
          if (scope.instruments.length > 0) {
            const query = {
              workspace: scope.workspace.ref(),
              inputStateId: scope.plug.stateId,
              instruments: scope.instruments };
            const json = JSON.stringify(query);
            if (json !== lastJson) {
              scope.result = util.get('/ajax/getInstrumentedState', query);
              const currentRequest = scope.result;
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
          setVisualizationEditHandler();
        }
        scope.onBlur = function() {
          $timeout(update); // Allow for changes to propagate to local scope.
        };

        scope.getDefaultSnapshotName = function() {
          return scope.workspace.name + '-' + scope.plugId;
        };

        scope.setInstrument = function(index, operationId, parameters) {
          scope.instruments.splice(index);
          scope.instruments.push({
            operationId: operationId,
            parameters: parameters || {},
            parametricParameters: {},
          });
          update();
        };

        scope.clearInstrument = function(index) {
          scope.instruments.splice(index);
          update();
        };

        scope.createSnapshot = function(stateId, saveAsName, done) {
          util.post('/ajax/createSnapshot', {
            name: saveAsName,
            id: stateId,
          }).finally(done).then(() => scope.root.snapshotNameOpen = false);
        };

        scope.graphray = function() {
          scope.$broadcast('graphray');
        };

        function setVisualizationEditHandler() {
          const n = scope.instruments.length;
          scope.visualizationEditHandler = {};
          if (n === 0) {
            const op =
              scope.workspace && scope.workspace.boxMap[scope.plug.boxId].metadata.operationId;
            if (op === 'Graph visualization') {
              scope.visualizationEditHandler.onSaveEdit = function(state) {
                scope.workspace.updateBox(scope.plug.boxId, { state }, {});
              };
            }
          } else {
            const op = scope.instruments[n - 1].operationId;
            if (op === 'Graph visualization') {
              scope.visualizationEditHandler.onEdit = function(state) {
                scope.setInstrument(n - 1, 'Graph visualization', { state });
              };
            }
          }
        }
      },
    };
  });
