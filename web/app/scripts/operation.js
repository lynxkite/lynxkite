'use strict';

angular.module('biggraph').directive('operation', function ($resource, util) {
  return {
    restrict: 'E',
    scope: { op: '=', side: '=' },
    replace: false,
    templateUrl: 'operation.html',
    link: function(scope) {
      scope.params = {};
      util.deepWatch(scope, 'op', function() {
        scope.op.parameters.forEach(function(p) {
          scope.params[p.id] = p.defaultValue;
        });
      });

      scope.apply = function() {
        if (!scope.op.enabled.success) { return; }
        var req = { project: scope.side.project.name, op: { id: scope.op.id, parameters: scope.params } };
        scope.running = true;
        // TODO: Report errors on the UI.
        $resource('/ajax/projectOp').save(req, function(result) {
          scope.running = false;
          if (result.success) {
            scope.side.reload();
          } else {
            console.error(result.failureReason);
          }
        }, function(response) {
          scope.running = false;
          console.error(response);
        });
      };
    }
  };
});
