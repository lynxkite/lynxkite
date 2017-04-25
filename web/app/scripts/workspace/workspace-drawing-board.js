'use strict';

// The drawing board where the user can create and modify a boxes and
// arrows diagram.

angular.module('biggraph')
  .directive('workspaceDrawingBoard', function() {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/workspace-drawing-board.html',
      templateNamespace: 'svg',
      scope: {
        workspace: '=',
      },
      link: function(scope, element) {
        var workspaceDrag = false;
        var selectBoxes = false;
        var workspaceX = 0;
        var workspaceY = 0;
        var workspaceZoom = 0;
        var mouseX = 0;
        var mouseY = 0;
        function zoomToScale(z) { return Math.exp(z * 0.001); }
        function addLogicalMousePosition(event) {
          /* eslint-disable no-console */
          console.assert(!('logicalX' in event) && !('logicalY' in event));
          console.assert(!('workspaceX' in event) && !('workspaceY' in event));
          // event.offsetX/Y are distorted when the mouse is
          // over a popup window (even if over an invisible
          // overflow part of it), hence we compute our own:
          event.workspaceX = event.pageX - element.offset().left;
          event.workspaceY = event.pageY - element.offset().top;
          // Add location according to pan and zoom:
          event.logicalX = (event.workspaceX - workspaceX) / zoomToScale(workspaceZoom);
          event.logicalY = (event.workspaceY - workspaceY) / zoomToScale(workspaceZoom);
        }
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
            scope.workspace.selection.endX = event.logicalX;
            scope.workspace.selection.endY = event.logicalY;
            scope.workspace.updateSelection();
            scope.workspace.selectBoxesInSelection();
          }
          mouseX = event.workspaceX;
          mouseY = event.workspaceY;
          scope.workspace.onMouseMove(event);
        };

        scope.onMouseDownOnBox = function(box, event) {
          event.stopPropagation();
          addLogicalMousePosition(event);
          scope.workspace.removeSelection();
          scope.workspace.onMouseDownOnBox(box, event);
        };

        scope.onMouseUp = function(event) {
          element[0].style.cursor = '';
          workspaceDrag = false;
          selectBoxes = false;
          scope.workspace.removeSelection();
          addLogicalMousePosition(event);
          scope.workspace.onMouseUp(event);
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
            scope.workspace.selectedBoxIds = [];
            scope.workspace.selection.endX = event.logicalX;
            scope.workspace.selection.endY = event.logicalY;
            scope.workspace.selection.startX = event.logicalX;
            scope.workspace.selection.startY = event.logicalY;
            scope.workspace.updateSelection();
          }
        };

        scope.workspaceTransform = function() {
          var z = zoomToScale(workspaceZoom);
          return 'translate(' + workspaceX + ', ' + workspaceY + ') scale(' + z + ')';
        };

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
          var operationID = event.originalEvent.dataTransfer.getData('text');
          // This isn't undefined iff testing
          var boxID = event.originalEvent.dataTransfer.getData('id');
          // This is received from operation-selector-entry.js
          scope.$apply(function() {
            addLogicalMousePosition(origEvent);
            scope.workspace.addBox(operationID, origEvent, boxID);
          });
        });

        scope.$on('$destroy', function() {
          scope.workspace.stopProgressUpdate();
        });
      }
    };
  });
