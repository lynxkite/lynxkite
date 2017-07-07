// An entry in the operation selector list. Supports dragging operations from here into
// the workspace drawing board.
'use strict';

angular.module('biggraph').directive('operationSelectorEntry', function() {
  return {
    restrict: 'E',
    scope: {
      ondrag: '&',
      op: '=',
      name: '=',
    },
    templateUrl: 'scripts/workspace/operation-selector-entry.html',
  };
});
