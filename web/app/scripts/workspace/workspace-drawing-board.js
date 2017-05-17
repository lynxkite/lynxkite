'use strict';

// The drawing board where the user can create and modify a boxes and
// arrows diagram.

angular.module('biggraph')
  .directive(
  'workspaceDrawingBoard',
  function(environment, hotkeys, PopupModel, SelectionModel, WorkspaceWrapper, $rootScope) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/workspace-drawing-board.html',
      scope: {
        boxCatalog: '=',
        workspaceName: '=',
      },
      link: function(scope, element) {
        scope.workspace = undefined;
        scope.selection = new SelectionModel();
        scope.clipboard = [];
        scope.dragMode = window.localStorage.getItem('drag_mode') || 'pan';
        scope.selectedBoxIds = [];
        scope.movedBoxes = undefined;
        // If the user is connecting plugs by drawing a line with the
        // mouse, then this points to the plug where the line was
        // started.
        scope.pulledPlug = undefined;
        // The last known position of the mouse, expressed in logical
        // workspace coordinates.
        scope.mouseLogical = undefined;
        scope.popups = [];
        scope.movedPopup = undefined;

        scope.$watchGroup(
          ['boxCatalog.$resolved', 'workspaceName'],

          function() {
            if (scope.boxCatalog.$resolved && scope.workspaceName) {
              scope.workspace = new WorkspaceWrapper(
                scope.workspaceName,
                scope.boxCatalog);
              scope.workspace.loadWorkspace();
            }
          });
        scope.$watch(
          'dragMode',
          function(dragMode) {
            window.localStorage.setItem('drag_mode', dragMode);
          });

        var workspaceDrag = false;
        var selectBoxes = false;
        var workspaceX = 0;
        var workspaceY = 0;
        var workspaceZoom = 0;
        var mouseX = 0;
        var mouseY = 0;
        var svgElement = element.find('svg');
        function zoomToScale(z) { return Math.exp(z * 0.001); }
        function addLogicalMousePosition(event) {
          /* eslint-disable no-console */
          console.assert(!('logicalX' in event) && !('logicalY' in event));
          console.assert(!('workspaceX' in event) && !('workspaceY' in event));
          // event.offsetX/Y are distorted when the mouse is
          // over a popup window (even if over an invisible
          // overflow part of it), hence we compute our own:
          event.workspaceX = event.pageX - svgElement.offset().left;
          event.workspaceY = event.pageY - svgElement.offset().top;
          // Add location according to pan and zoom:
          var logical = scope.pageToLogical({ x: event.pageX, y: event.pageY });
          event.logicalX = logical.x;
          event.logicalY = logical.y;
          return event;
        }

        scope.pageToLogical = function(pos) {
          var z = zoomToScale(workspaceZoom);
          var offset = svgElement.offset();
          return {
            x: (pos.x - offset.left - workspaceX) / z,
            y: (pos.y - offset.top - workspaceY) / z,
          };
        };

        scope.logicalToPage = function(pos) {
          var z = zoomToScale(workspaceZoom);
          var offset = svgElement.offset();
          return {
            x: pos.x * z + workspaceX + offset.left,
            y: pos.y * z + workspaceY + offset.top,
          };
        };

        function actualDragMode(event) {
          var dragMode = (window.localStorage.getItem('drag_mode') || 'pan');
          // Shift chooses the opposite mode.
          if (dragMode === 'select') {
            return event.shiftKey ? 'pan' : 'select';
          } else {
            return event.shiftKey ? 'select' : 'pan';
          }
        }

        scope.onMouseMove = function(event) {
          event.preventDefault();
          addLogicalMousePosition(event);
          if (workspaceDrag) {
            workspaceX += event.workspaceX - mouseX;
            workspaceY += event.workspaceY - mouseY;
          } else if (selectBoxes) {
            scope.selection.onMouseMove(event);
            scope.selectBoxesInSelection();
          }
          mouseX = event.workspaceX;
          mouseY = event.workspaceY;

          var leftButton = event.buttons & 1;
          // Protractor omits button data from simulated mouse events.
          if (!leftButton && !environment.protractor) {
            // Button is no longer pressed. (It was released outside of the window, for example.)
            scope.onMouseUp();
          } else {
            scope.mouseLogical = {
              x: event.logicalX,
              y: event.logicalY,
            };
            if (scope.movedBoxes) {
              for (var i = 0; i < scope.movedBoxes.length; i++) {
                scope.movedBoxes[i].onMouseMove(event);
              }
            } else if (scope.movedPopup) {
              scope.movedPopup.onMouseMove(event);
            }
          }
        };

        scope.onMouseDownOnBox = function(box, event) {
          event.stopPropagation();
          addLogicalMousePosition(event);
          scope.selection.remove();
          if (scope.selectedBoxIds.indexOf(box.instance.id) === -1) {
            if (!event.ctrlKey) {
              scope.selectedBoxIds = [];
            }
            scope.selectBox(box.instance.id);
            scope.movedBoxes = [box];
            scope.movedBoxes[0].onMouseDown(event);
          } else if (event.ctrlKey) {
            var selectedIndex = scope.selectedBoxIds.indexOf(box.instance.id);
            scope.selectedBoxIds.splice(selectedIndex, selectedIndex);
            scope.movedBoxes[0].onMouseDown(event);
          } else {
            scope.movedBoxes = this.selectedBoxes();
            scope.movedBoxes.map(function(b) {
              b.onMouseDown(event);
            });
          }
        };

        scope.onMouseUpOnBox = function(box, event) {
          if (box.isMoved || scope.pulledPlug) {
            return;
          }
          var leftButton = event.button === 0;
          if (!leftButton || event.ctrlKey || event.shiftKey) {
            return;
          }
          var model = new PopupModel(
            box.instance.id,
            box.instance.operationId,
            {
              type: 'box',
              boxId: box.instance.id,
            },
            event.pageX - 200,
            event.pageY + 60,
            500,
            500,
            scope);
          model.toggle();
        };

        scope.closePopup = function(id) {
          for (var i = 0; i < scope.popups.length; ++i) {
            if (scope.popups[i].id === id) {
              scope.popups.splice(i, 1);
              return true;
            }
          }
          return false;
        };

        scope.onClickOnPlug = function(plug, event) {
          var leftButton = event.button === 0;
          if (!leftButton || event.ctrlKey || event.shiftKey) {
            return;
          }
          event.stopPropagation();
          if (plug.direction === 'outputs') {
            var model = new PopupModel(
              plug.boxId + '_' + plug.id,
              plug.boxInstance.operationId + ' ➡ ' + plug.id,
              {
                type: 'plug',
                boxId: plug.boxId,
                plugId: plug.id,
              },
              event.pageX - 300,
              event.pageY + 15,
              500,
              500,
              scope);
            model.toggle();
          }
        };

        scope.onMouseDownOnPlug = function(plug, event) {
          event.stopPropagation();
          scope.pulledPlug = plug;
          scope.mouseLogical = undefined;
        };

        scope.onMouseUpOnPlug = function(plug, event) {
          event.stopPropagation();
          if (scope.pulledPlug) {
            var otherPlug = scope.pulledPlug;
            scope.pulledPlug = undefined;
            scope.workspace.addArrow(otherPlug, plug);
          }
        };

        scope.onMouseUp = function() {
          element[0].style.cursor = '';
          workspaceDrag = false;
          selectBoxes = false;
          scope.selection.remove();
          if (scope.movedBoxes) {
            scope.workspace.saveIfBoxesMoved();
          }
          scope.movedBoxes = undefined;
          scope.pulledPlug = undefined;
          scope.movedPopup = undefined;
        };

        scope.onMouseDown = function(event) {
          var dragMode = actualDragMode(event);
          event.preventDefault();
          addLogicalMousePosition(event);
          if (dragMode === 'pan') {
            workspaceDrag = true;
            setGrabCursor(element[0]);
            mouseX = event.workspaceX;
            mouseY = event.workspaceY;
          } else if (dragMode === 'select') {
            selectBoxes = true;
            scope.selectedBoxIds = [];
            scope.selection.onMouseDown(event);
          }
        };

        scope.workspaceTransform = function() {
          var z = zoomToScale(workspaceZoom);
          return 'translate(' + workspaceX + ', ' + workspaceY + ') scale(' + z + ')';
        };

        scope.boxes = function() {
          return scope.workspace ? scope.workspace.boxes : [];
        };

        scope.arrows = function() {
          return scope.workspace ? scope.workspace.arrows : [];
        };

        scope.selectBoxesInSelection = function() {
          var boxes = this.boxes();
          this.selectedBoxIds = [];
          for (var i = 0; i < boxes.length; i++) {
            var box = boxes[i];
            if (this.selection.inSelection(box)) {
              this.selectedBoxIds.push(box.instance.id);
            }
          }
        };

        scope.selectBox = function(boxId) {
          scope.selectedBoxIds.push(boxId);
        };

        scope.selectedBoxes = function() {
          if (scope.selectedBoxIds) {
            return scope.selectedBoxIds.map(function(id) {
              return scope.workspace.boxMap[id];
            });
          } else {
            return undefined;
          }
        };

        scope.copyBoxes = function() {
          scope.clipboard = angular.copy(scope.selectedBoxes());
        };

        scope.pasteBoxes = function() {
          var pos = addLogicalMousePosition({ pageX: 0, pageY: 0});
          scope.workspace.pasteFromClipboard(scope.clipboard, pos);
        };

        scope.deleteBoxes = function(boxIds) {
          var popups = scope.popups.slice();
          popups.forEach(function(popup) {
            var boxId = popup.content.boxId;
            if (boxIds.includes(boxId) && boxId !== 'anchor') {
              scope.closePopup(popup.id);
            }
          });
          scope.workspace.deleteBoxes(boxIds);
        };

        scope.deleteSelectedBoxes = function() {
          this.deleteBoxes(this.selectedBoxIds);
          this.selectedBoxIds = [];
        };

        scope.diveUp = function() {
          scope.workspace.customBoxStack.pop();
          scope.workspace.loadWorkspace();
          scope.popups = [];
        };

        scope.diveDown = function() {
          scope.workspace.customBoxStack.push(scope.selectedBoxIds[0]);
          scope.workspace.loadWorkspace();
          scope.popups = [];
        };

        var hk = hotkeys.bindTo(scope);
        hk.add({
          combo: 'ctrl+c', description: 'Copy boxes',
          callback: function() { scope.copyBoxes(); } });
        hk.add({
          combo: 'ctrl+v', description: 'Paste boxes',
          callback: function() { scope.pasteBoxes(); } });
        hk.add({
          combo: 'ctrl+z', description: 'Undo',
          callback: function() { scope.workspace.undo(); } });
        hk.add({
          combo: 'ctrl+y', description: 'Redo',
          callback: function() { scope.workspace.redo(); } });
        hk.add({
          combo: 'del', description: 'Paste boxes',
          callback: function() { scope.deleteSelectedBoxes(); } });
        hk.add({
          combo: '/', description: 'Find operation',
          callback: function(e) {
            e.preventDefault();  // Do not type "/".
            $rootScope.$broadcast('open operation search');
          }});

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

        element.find('svg').on('wheel', function(event) {
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
            workspaceX = mouseX - (mouseX - workspaceX) * z2 / z1;
            workspaceY = mouseY - (mouseY - workspaceY) * z2 / z1;
          });
        });
        element.bind('dragover', function(event) {
          event.preventDefault();
        });
        element.bind('drop', function(event) {
          event.preventDefault();
          var origEvent = event.originalEvent;
          var operationId = event.originalEvent.dataTransfer.getData('text');
          // This isn't undefined iff testing
          var boxId = event.originalEvent.dataTransfer.getData('id');
          // This is received from operation-selector-entry.js
          scope.$apply(function() {
            addLogicalMousePosition(origEvent);
            scope.workspace.addBox(operationId, origEvent, boxId);
          });
        });

        scope.$on('$destroy', function() {
          scope.workspace.stopProgressUpdate();
        });
      }
    };
  });
