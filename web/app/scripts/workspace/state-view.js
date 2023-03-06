'use strict';
import '../app';
import '../util/util';

// Viewer of a state at an output of a box.

angular.module('biggraph')
  .directive('stateView', function(util, $timeout, $rootScope) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/state-view.html',
      scope: {
        workspace: '=',
        plug: '=',
        popupModel: '=',
        interactive: '@?',
        wizard: '=?',
      },
      link: function(scope) {
        scope.util = util;
        const instruments = []; // Internal state.
        scope.instruments = []; // Displayed state. Updated on backend response.
        scope.root = { snapshotNameOpen: false }; // Dealing with ng-if scopes.
        scope.$watch('plug.stateId', update);
        scope.$on('bring popup to front', () => scope.popupModel.bringToFront({ buttons: 1 }));
        let lastJson;

        function update() {
          if (instruments.length > 0) {
            const query = {
              workspace: scope.workspace && scope.workspace.ref(),
              inputStateId: scope.plug.stateId,
              instruments };
            const json = JSON.stringify(query);
            if (json !== lastJson) {
              const req = util.get('/ajax/getInstrumentedState', query);
              scope.nextResult = req;
              req.then(function(res) {
                if (scope.nextResult === req) { // It is not an abandoned request.
                  scope.result = req;
                  scope.instruments = instruments.slice();
                  scope.lastState = res.states[res.states.length - 1];
                }
              }).catch(function(err) {
                util.ajaxError(err);
              });
              lastJson = json;
            }
          } else {
            scope.result = { states: [scope.plug], metas: [] };
            scope.lastState = scope.plug;
            scope.instruments = [];
            lastJson = undefined;
          }
          setVisualizationEditHandler();
        }
        scope.onBlur = function() {
          $timeout(update); // Allow for changes to propagate to local scope.
        };

        scope.getDefaultSnapshotName = function() {
          return scope.workspace ? scope.workspace.name + '-' + scope.plugId : scope.plugId;
        };

        scope.setInstrument = function(index, operationId, parameters) {
          instruments.splice(index);
          instruments.push({
            operationId: operationId,
            parameters: parameters || {},
            parametricParameters: {},
          });
          update();
        };

        scope.clearInstrument = function(index) {
          instruments.splice(index);
          update();
        };

        scope.createSnapshot = function(stateId, saveAsName, done) {
          util.post('/ajax/createSnapshot', {
            name: saveAsName,
            id: stateId,
          }).finally(done).then(() => {
            scope.root.snapshotNameOpen = false;
            $rootScope.$broadcast('saved snapshot', saveAsName);
          });
        };

        scope.graphray = function() {
          scope.$broadcast('graphray');
        };

        function setVisualizationEditHandler() {
          const n = scope.instruments.length;
          scope.visualizationEditHandler = {};
          if (n === 0) {
            const op = scope.interactive &&
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
