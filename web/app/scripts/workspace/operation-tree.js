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
      };

      // Create a tree from the path fragments of the operations.
      for (var opI in scope.ops) {
        var operation = scope.ops[opI];
        var opPath = operation.operationId.split('/');
        var nodeI = scope.node;
        for (var i = 0; i < opPath.length - 1; i++) {
          var opPathI = opPath[i];
          if (!(opPathI in nodeI.dirs)) {
            nodeI.dirs[opPathI] = {
              dirs: {},
              ops: {},
              baseName: opPathI,
              isOpen: false,
              toggle: function() { this.isOpen = !this.isOpen; },
            };
          }
          nodeI = nodeI.dirs[opPathI];
        }
        var opBaseName = opPath[opPath.length - 1];
        nodeI.ops[opBaseName] = {
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
