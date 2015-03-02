'use strict';

angular.module('biggraph').directive('operation', function(util, hotkeys) {
  return {
    restrict: 'E',
    scope: { op: '=', side: '=' },
    templateUrl: 'operation.html',
    link: function(scope, element) {
      scope.params = {};
      scope.fileUploads = {};
      scope.$watch('op.parameters', function() {
        for (var i = 0; i < scope.op.parameters.length; ++i) {
          var p = scope.op.parameters[i];
          if (p.options.length === 0) {
            scope.params[p.id] = p.defaultValue;
          } else if (p.multipleChoice) {
            scope.params[p.id] = [];
          } else {
            scope.params[p.id] = p.options[0].id;
          }
        }
      });

      scope.apply = function() {
        if (!scope.op.status.enabled || scope.running || scope.busy) {
          return;
        }
        var reqParams = {};
        scope.op.parameters.forEach(function(p) {
          if (p.multipleChoice) {
            reqParams[p.id] = (scope.params[p.id] || []).join(',');
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

      // Focus the first input box when the operation is opened.
      scope.$watch(function() {
        // Have to watch for the parameters to finish rendering.
        return element.find('input, select')[0];
      }, function(firstInput) {
        if (firstInput) {
          firstInput.select();
        } else {
          // No parameters? Focus on the OK button.
          element.find('.ok-button').focus();
        }
      });
    }
  };
});
