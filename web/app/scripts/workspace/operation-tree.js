// An entry in the operation selector list. Supports dragging operations from here into
// the workspace drawing board.
'use strict';

angular.module('biggraph').directive('operationTree', function() {
  return {
    restrict: 'E',
    scope: {
      ops: '=',
      ondrag: '&',
    },
    templateUrl: 'scripts/workspace/operation-tree.html',
    link: function(scope) {
      scope.node = {
        dirs: {},
        ops: {},
        isOpen: true,
      };

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
