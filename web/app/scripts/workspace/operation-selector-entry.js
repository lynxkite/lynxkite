// An entry in the operation selector list. Supports dragging operations from here into
// the workspace drawing board.
'use strict';

angular.module('biggraph').directive('operationSelectorEntry', function() {
  return {
    restrict: 'E',
    scope: {
      op: '=',
    },
    templateUrl: 'scripts/workspace/operation-selector-entry.html',
    link: function(scope, element) {
      element.bind('dragstart', function(event) {
        // We send the ID of the box over drag-and-drop.
        // This will be received in workspace-board.js
        event.originalEvent.dataTransfer.setData(
            'text',
            scope.op.operationID);
      });
    }
  };
});

