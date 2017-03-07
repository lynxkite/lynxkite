'use strict';

angular.module('biggraph')
 .directive('workspaceBoard', function(createWorkspace, util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/workspace-board.html',
      templateNamespace: 'svg',
      scope: {
        workspaceName: '=',
        selectedBox: '=',
        selectedState: '=',
        boxCatalog: '=',
      },
      link: function(scope, element) {

        scope.$watchGroup(
          ['boxCatalog.$resolved', 'workspaceName'],
          function() {
            if (scope.boxCatalog.$resolved && scope.workspaceName) {
              scope.boxCatalogMap = {};
              for (var i = 0; i < scope.boxCatalog.boxes.length; ++i) {
                var boxMeta = scope.boxCatalog.boxes[i];
                scope.boxCatalogMap[boxMeta.operationID] = boxMeta;
              }

              scope.loadWorkspace();
            }
        });

        scope.loadWorkspace = function() {
          util.nocache(
              '/ajax/getWorkspace',
              {
                name: scope.workspaceName
              })
              .then(function(rawWorkspace) {
                scope.workspace = createWorkspace(
                    rawWorkspace, scope.boxCatalogMap);
                scope.selectBox(scope.selectedBoxId);
              });
        };

        scope.saveWorkspace = function() {
          util.post(
            '/ajax/setWorkspace',
            {
              name: scope.workspaceName,
              workspace: scope.workspace.rawWorkspace(),
            }).then(
              // Reload workspace both in error and success cases.
              scope.loadWorkspace,
              scope.loadWorkspace);
        };

        scope.selectBox = function(boxId) {
          scope.selectedBox = undefined;
          scope.selectedBoxId = undefined;
          if (!boxId) {
            return;
          }
          scope.selectedBox = scope.workspace.boxMap[boxId];
          scope.selectedBoxId = boxId;
        };
        scope.selectState = function(boxID, outputID) {
          scope.selectedState = util.nocache(
              '/ajax/getOutput',
              {
                  workspace: scope.workspaceName,
                  output: {
                    boxID: boxID,
                    id: outputID
                  }
              });
        };
        scope.selectPlug = function(plug) {
          scope.selectedPlug = plug;
          if (plug.direction === 'outputs') {
            scope.selectState(plug.boxId, plug.data.id);
          } else {
            scope.selectedState = undefined;
          }
        };
        scope.onMouseMove = function(event) {
          scope.mouseX = event.offsetX;
          scope.mouseY = event.offsetY;
          if (event.buttons === 1 && scope.movedBox) {
            scope.movedBox.onMouseMove(event);
          }
        };
        scope.onMouseUp = function() {
          if (scope.movedBox && scope.movedBox.isMoved) {
            scope.saveWorkspace();
          }
          scope.movedBox = undefined;
          scope.pulledPlug = undefined;
        };
        scope.onMouseDownOnBox = function(box, event) {
          scope.selectBox(box.instance.id);
          scope.movedBox = box;
          scope.movedBox.onMouseDown(event);
        };
        scope.onMouseDownOnPlug = function(plug, event) {
          event.stopPropagation();
          scope.pulledPlug = plug;
        };
        scope.onMouseUpOnPlug = function(plug, event) {
          event.stopPropagation();
          if (scope.pulledPlug) {
            var otherPlug = scope.pulledPlug;
            scope.pulledPlug = undefined;
            if (scope.workspace.addArrow(otherPlug, plug)) {
              scope.saveWorkspace();
            }
          }
          if (!scope.pulledPlug || scope.pulledPlug !== plug) {
            scope.selectPlug(plug);
          }
        };

        scope.addBox = function(operationId, x, y) {
          scope.workspace.addBox(operationId, x, y);
          scope.saveWorkspace();
        };
        element.bind('dragover', function(event) {
          event.preventDefault();
        });
        element.bind('drop', function(event) {
          event.preventDefault();
          var origEvent = event.originalEvent;
          var operationID = event.originalEvent.dataTransfer.getData('text');
          // This is received from operation-selector-entry.js
          scope.$apply(function() {
            scope.addBox(operationID, origEvent.offsetX, origEvent.offsetY);
          });
        });
      }

    };

});
