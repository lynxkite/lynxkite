'use strict';

// The drawing board where the user can create and modify a boxes and
// arrows diagram.
//
// Life cycle:
// 1. boxCatalog needs to be loaded at all times for things to work
// 2. loadWorkspace()
//    - downloads a workspace
//    - triggers workspace.build()
//    - sets scope.workspace to the downloaded and built workspace
//    - visible GUI gets updated
// 3. user edit happens, e.g. box move, add box, or add arrow
// 4. in cases of "complex edits" - edits except for move:
//    - scope.workspace.build() is called by the edit code
//    - this updates the visible GUI immediately
// 5. saveWorkspace()
// 6. GOTO 2

angular.module('biggraph')
  .directive('workspaceDrawingBoard', function(createWorkspace, util, $interval) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/workspace-drawing-board.html',
      templateNamespace: 'svg',
      scope: {
        workspaceName: '=',
        selectedBox: '=',
        selectedState: '=',
        boxCatalog: '=',
      },
      link: function(scope, element) {
        var progressUpdater;

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

        scope.updateProgress = function() {
          var workspaceBefore = scope.workspace;
          var plugBefore = scope.selectedPlug;
          if (workspaceBefore && plugBefore && plugBefore.direction === 'outputs') {
            util.nocache('/ajax/getProgress', {
              workspace: scope.workspaceName,
              output: {
                boxID: plugBefore.boxId,
                id: plugBefore.data.id
              }
            }).then(
              /* success */
              function(response) {
                var workspace = scope.workspace;
                var selectedPlug = scope.selectedPlug;
                if (workspace && workspace === workspaceBefore &&
                    selectedPlug && selectedPlug === plugBefore) {
                  var progressMap = response.progressMap;
                  for (var i = 0; i < progressMap.length; i++) {
                    var output =  progressMap[i];
                    var boxID = output.a.boxID;
                    var plugID = output.a.id;
                    var box = workspace.boxMap[boxID];
                    var plug = box.outputMap[plugID];
                    plug.onUpdatedProgress(output.b);
                  }
                }
              },
              /* failure */
              function(error) {
                scope.error = error;
              });
          }
        };

        progressUpdater = $interval(scope.updateProgress, 2000);

        scope.$on('$destroy', function() {
          $interval.cancel(progressUpdater);
          progressUpdater = undefined;
        });
      }
    };
  });
