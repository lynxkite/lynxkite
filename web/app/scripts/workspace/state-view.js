'use strict';

// Viewer of a state at an output of a box.

angular.module('biggraph')
  .directive('stateView', function(util, WorkspaceWrapper) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/state-view.html',
      scope: {
        workspace: '=',
        boxId: '=',
        plugId: '=',
        popupModel: '=',
      },
      link: function(scope) {
        scope.instrument = undefined;

        scope.$watch(function() {
          if (scope.boxId && scope.plugId && scope.workspace) {
            scope.plug = scope.workspace.getOutputPlug(scope.boxId, scope.plugId);
            return scope.plug.stateId;
          } else {
            scope.plug = undefined;
            return undefined;
          }
        });

        scope.createSnapshot = function(saveAsName, success, error) {
          var postOpts = { reportErrors: false };
          util.post('/ajax/createSnapshot', {
            name: saveAsName,
            id: scope.plug.stateId,
          }, postOpts).then(success, error);
        };

        scope.$watch('instrument', function(instrument) {
          if (!instrument) {
            return;
          }
          // We add the instrument box to a copy of the current workspace saved in the "tmp"
          // directory. TODO: Do this without temporary workspace.
          var ws = scope.workspace;
          var tmpBox = instrument + ' ' + Date();
          var tmpWS = 'tmp/' + ws.name + '/' + tmpBox;
          var boxes = ws.backendState.boxes.slice();
          var box = {
            id: tmpBox,
            operationId: instrument,
            x: 500,
            y: 500,
            inputs: {},
            parameters: {},
            parametricParameters: {}
          };
          var inputName = ws._boxCatalogMap[instrument].inputs[0];
          var outputName = ws._boxCatalogMap[instrument].outputs[0];
          box.inputs[inputName] = {
            boxId: scope.boxId,
            id: scope.plugId,
          };
          boxes.push(box);

          util.post('/ajax/createWorkspace', {
            name: tmpWS,
          }).then(function success() {
            return util.post('/ajax/setWorkspace', {
              name: tmpWS,
              workspace: { boxes: boxes },
            });
          }).then(function success() {
            scope.instrumentWorkspace = new WorkspaceWrapper(tmpWS, ws.boxCatalog);
            scope.instrumentWorkspace.loadWorkspace();
            scope.instrumentBoxId = tmpBox;
            scope.instrumentPlugId = outputName;
          });
        });

        scope.$watch(function() {
          if (scope.instrumentWorkspace) {
            var box = scope.instrumentWorkspace.getBox(scope.instrumentBoxId);
            if (box) {
              return box.outputMap[scope.instrumentPlugId].stateId;
            }
          }
        }, function(stateId) {
          if (stateId) {
            scope.instrumentStateId = stateId;
            var box = scope.instrumentWorkspace.getBox(scope.instrumentBoxId);
            scope.instrumentStateKind = box.outputMap[scope.instrumentPlugId].kind;
          }
        });
      },
    };
  });
