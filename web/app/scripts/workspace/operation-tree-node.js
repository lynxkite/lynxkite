// A directory in the directory tree based operation browser.
'use strict';
import '../app';
import templateUrl from './operation-tree-node.html?url';

angular.module('biggraph').directive('operationTreeNode', function() {
  return {
    restrict: 'E',
    scope: {
      node: '=', // Browser tree node representing a dir.
    },
    templateUrl,
  };
});
