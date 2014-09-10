'use strict';

angular.module('biggraph').directive('operation', function (util, hotkeys) {
  return {
    restrict: 'E',
    scope: { op: '=', side: '=' },
    replace: false,
    templateUrl: 'operation.html',
    link: function(scope) {
      scope.params = {};
      scope.multiParams = {};
      util.deepWatch(scope, 'op', function() {
        scope.op.parameters.forEach(function(p) {
          if (p.options.length === 0) {
            scope.params[p.id] = p.defaultValue;
          } else {
            if (!p.multipleChoice) {
              scope.params[p.id] = p.options[0].id;
            }
          }
        });
      });

      scope.apply = function() {
        if (!scope.op.status.enabled) { return; }
        var reqParams = {};
        scope.op.parameters.forEach(function(p) {
          if (p.multipleChoice) {
            reqParams[p.id] = scope.multiParams[p.id].join(',');
          } else {
            reqParams[p.id] = scope.params[p.id];
          }
        });
        scope.running = true;
        scope.side.applyOp(scope.op.id, reqParams)
          .then(function() { scope.running = false; });
      };
      hotkeys.bindTo(scope).add({
        combo: 'enter',
        callback: scope.apply,
      });
    }
  };
});
