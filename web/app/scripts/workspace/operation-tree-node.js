// An entry in the operation selector list. Supports dragging operations from here into
// the workspace drawing board.
'use strict';

angular.module('biggraph').directive('operationTreeNode', function() {
  return {
    restrict: 'E',
    scope: {
      node: '=',
    },
    templateUrl: 'scripts/workspace/operation-tree-node.html',
  };
});
