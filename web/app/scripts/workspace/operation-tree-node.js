// A directory in the directory tree based operation browser.
'use strict';

angular.module('biggraph').directive('operationTreeNode', function() {
  return {
    restrict: 'E',
    scope: {
      node: '=', // Browser tree node representing a dir.
    },
    templateUrl: 'scripts/workspace/operation-tree-node.html',
  };
});
