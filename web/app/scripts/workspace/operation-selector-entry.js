// An entry in the operation selector list. Supports dragging operations from here into
// the workspace drawing board.
'use strict';

angular.module('biggraph').directive('operationSelectorEntry', function() {
  return {
    restrict: 'E',
    scope: {
      ondrag: '&', // The function to handle dragging.
      op: '=',     // The actual underlying operation.
      name: '=',   // Name to display on the UI.
    },
    templateUrl: 'scripts/workspace/operation-selector-entry.html',
  };
});
