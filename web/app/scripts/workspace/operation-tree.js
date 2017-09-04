// A browser tree for operations to be browsed as a directory tree, e.g. custom boxes.
'use strict';

angular.module('biggraph').directive('operationTree', function() {
  return {
    restrict: 'E',
    scope: {
      ops: '=', // The flattened list of operations to be converted to a tree.
      ondrag: '&',
    },
    templateUrl: 'scripts/workspace/operation-tree.html',
    link: function(scope) {
      scope.node = {
        dirs: {},
        ops: {},
        isOpen: true,
        indent: false,
      };

      // Create a tree from the path fragments of the operations.
      for (var idx in scope.ops) {
        var operation = scope.ops[idx];
        var opPathParts = operation.operationId.split('/');
        var currentNode = scope.node;

        // Iterate until one before the last part, and create non-leaf nodes.
        for (var i = 0; i < opPathParts.length - 1; i++) {
          var opPathPart = opPathParts[i];
          // Create a node for the dir if not exists.
          if (!(opPathPart in currentNode.dirs)) {
            currentNode.dirs[opPathPart] = {
              dirs: {},
              ops: {},
              baseName: opPathPart,
              isOpen: false,
              toggle: function() { this.isOpen = !this.isOpen; },
              indent: true,
            };
          }
          currentNode = currentNode.dirs[opPathPart];
        }

        // The last part becomes a leaf-node (it's the operation).
        var opBaseName = opPathParts[opPathParts.length - 1];
        currentNode.ops[opBaseName] = {
          op: operation,
          baseName: opBaseName,
          localOndrag: function(operation, $event) {
            scope.ondrag({ op: operation, $event: $event });
          },
        };
      }
    },
  };
});
