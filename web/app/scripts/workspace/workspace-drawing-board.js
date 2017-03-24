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
            })
            .then(function() {
              return util.nocache(
                '/ajax/getAllOutputIDs',
                {
                  workspaceName: scope.workspaceName
                });
            })
            .then(function(response) {
              var outputs = response.outputs;
              scope.workspace.assignStateIDsToPlugs(outputs);
            })
            .then(function() {
              scope.startProgressUpdate();
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
          var stateID = scope.workspace.boxMap[boxID].outputMap[outputID].stateID;
          scope.selectedState = util.nocache(
            '/ajax/getOutput',
            {
              id: stateID,
            }
          );
        };

        scope.selectPlug = function(plug) {
          scope.selectedPlug = plug;
          if (plug.direction === 'outputs') {
            scope.selectState(plug.boxId, plug.data.id);
          } else {
            scope.selectedState = undefined;
          }
        };

        var workspaceDrag = false;
        var workspaceX = 0;
        var workspaceY = 0;
        var workspaceZoom = 0;
        function zoomToScale(z) { return Math.exp(z * 0.001); }
        function getLogicalPosition(event) {
          return {
            x: (event.offsetX - workspaceX) / zoomToScale(workspaceZoom),
            y: (event.offsetY - workspaceY) / zoomToScale(workspaceZoom) };
        }

        scope.onMouseMove = function(event) {
          event.preventDefault();
          if (workspaceDrag) {
            workspaceX += event.offsetX - scope.mouseX;
            workspaceY += event.offsetY - scope.mouseY;
          }
          scope.mouseX = event.offsetX;
          scope.mouseY = event.offsetY;
          scope.mouseLogical = getLogicalPosition(event);
          if (event.buttons === 1 && scope.movedBox) {
            scope.movedBox.onMouseMove(scope.mouseLogical);
          }
        };

        scope.onMouseUp = function() {
          if (scope.movedBox && scope.movedBox.isMoved) {
            scope.saveWorkspace();
          }
          scope.movedBox = undefined;
          scope.pulledPlug = undefined;
          workspaceDrag = false;
          element[0].style.cursor = '';
        };

        scope.onMouseDown = function(event) {
          event.preventDefault();
          workspaceDrag = true;
          setGrabCursor(element[0]);
        };

        element.on('wheel', function(event) {
          event.preventDefault();
          var delta = event.originalEvent.deltaY;
          if (/Firefox/.test(window.navigator.userAgent)) {
            // Every browser sets different deltas for the same amount of scrolling.
            // It is tiny on Firefox. We need to boost it.
            delta *= 20;
          }
          scope.$apply(function() {
            var z1 = zoomToScale(workspaceZoom);
            workspaceZoom -= delta;
            var z2 = zoomToScale(workspaceZoom);
            // Maintain screen-coordinates of logical point under the mouse.
            workspaceX = scope.mouseX - (scope.mouseX - workspaceX) * z2 / z1;
            workspaceY = scope.mouseY - (scope.mouseY - workspaceY) * z2 / z1;
          });
        });

        function setGrabCursor(e) {
          // Trying to assign an invalid cursor will silently fail. Try to find a supported value.
          e.style.cursor = '';
          e.style.cursor = 'grabbing';
          if (!e.style.cursor) {
            e.style.cursor = '-webkit-grabbing';
          }
          if (!e.style.cursor) {
            e.style.cursor = '-moz-grabbing';
          }
        }

        scope.onMouseDownOnBox = function(box, event) {
          event.stopPropagation();
          scope.selectBox(box.instance.id);
          scope.movedBox = box;
          scope.movedBox.onMouseDown(getLogicalPosition(event));
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

        scope.addBox = function(operationId, pos) {
          scope.workspace.addBox(operationId, pos.x, pos.y);
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
            scope.addBox(operationID, getLogicalPosition(origEvent));
          });
        });

        scope.workspaceTransform = function() {
          var z = zoomToScale(workspaceZoom);
          return 'translate(' + workspaceX + ', ' + workspaceY + ') scale(' + z + ')';
        };

        scope.$on('box parameters updated', function(event, data) {
          scope.workspace.setBoxParams(data.boxId, data.paramValues);
          scope.saveWorkspace();
        });

        scope.getAndUpdateProgress = function(errorHandler) {
          var workspaceBefore = scope.workspace;
          if (workspaceBefore) {
            util.nocache('/ajax/getProgress', {
              stateIDs: workspaceBefore.knownStateIDs,
            }).then(
              function success(response) {
                if (scope.workspace && scope.workspace === workspaceBefore) {
                  scope.workspace.updateProgress(response.progressMap);
                }
              },
              errorHandler);
          }
        };

        scope.startProgressUpdate = function() {
          scope.stopProgressUpdate();
          progressUpdater = $interval(function() {
            function errorHandler(error) {
              util.error('Couldn\'t get progress information.', error);
              scope.stopProgressUpdate();
              scope.workspace.clearProgress();
            }
            scope.getAndUpdateProgress(errorHandler);
          }, 2000);
        };

        scope.stopProgressUpdate = function() {
          if (progressUpdater) {
            $interval.cancel(progressUpdater);
            progressUpdater = undefined;
          }
        };

        scope.$on('$destroy', function() {
          scope.stopProgressUpdate();
        });
      }
    };
  });
