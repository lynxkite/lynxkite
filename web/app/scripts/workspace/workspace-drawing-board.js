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
        function getLogicalPosition(event) {
          return {
            x: (event.offsetX - workspaceX) / zoomToScale(workspaceZoom),
            y: (event.offsetY - workspaceY) / zoomToScale(workspaceZoom) };
        }
        scope.onMouseMove = function(event) {
          event.preventDefault();
          if (workspaceDrag) {
            workspaceX += event.offsetX - mouseX;
            workspaceY += event.offsetY - mouseY;
          }
          if (selectBoxes) {
            var logicalPos = getLogicalPosition(event);
            scope.workspace.selectionBox.endX = logicalPos.x;
            scope.workspace.selectionBox.endY = logicalPos.y;
          }
          mouseX = event.offsetX;
          mouseY = event.offsetY;
          scope.workspace.onMouseMove(getLogicalPosition(event));
        };

        scope.Math = window.Math;

        scope.onMouseDownOnBox = function(box, event) {
          event.stopPropagation();
          scope.workspace.onMouseDownOnBox(box, getLogicalPosition(event));
        };

        scope.onMouseUp = function(event) {
          if(window.localStorage.getItem('grab_or_select') === 'grab'){
            workspaceDrag = false;
            element[0].style.cursor = '';
            scope.workspace.onMouseUp(getLogicalPosition(event));
          }
          else{
            selectBoxes = false;
            scope.workspace.onMouseUp(getLogicalPosition(event));
          }
        };

        scope.onMouseDown = function(event) {
          if(window.localStorage.getItem('grab_or_select') === 'grab'){
            event.preventDefault();
            workspaceDrag = true;
            setGrabCursor(element[0]);
            mouseX = event.offsetX;
            mouseY = event.offsetY;
          } else {
            event.preventDefault();
            selectBoxes = true;
            var logicalPos = getLogicalPosition(event);
            scope.workspace.selectionBox.endX = logicalPos.x;
            scope.workspace.selectionBox.endY = logicalPos.y;
            scope.workspace.selectionBox.startX = logicalPos.x;
            scope.workspace.selectionBox.startY = logicalPos.y;
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
          // This is received from operation-selector-entry.js
          scope.$apply(function() {
            scope.workspace.addBox(operationID, getLogicalPosition(origEvent));
          });
        });

        scope.$on('$destroy', function() {
          scope.workspace.stopProgressUpdate();
        });
      }
    };
  });
