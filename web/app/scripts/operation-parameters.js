'use strict';

angular.module('biggraph').directive('operationParameters', function(util) {
  return {
    restrict: 'E',
    scope: { input: '=', output: '=', busy: '=?' },
    templateUrl: 'operation-parameters.html',
    link: function(scope) {
      scope.fileUploads = { count: 0 };
      scope.$watch('fileUploads.count', function(count) {
        scope.busy = count !== 0;
      });
      // Translate between arrays and comma-separated strings for multiselects.
      scope.multiOutput = {};
      util.deepWatch(scope, 'output', function(output) {
        for (var i = 0; i < scope.input.length; ++i) {
          var param = scope.input[i];
          if (param.options.length > 0 && param.multipleChoice) {
            scope.multiOutput[param.id] = (output[param.id] || '').split(',');
          }
        }
      });
      util.deepWatch(scope, 'multiOutput', function(multiOutput) {
        for (var i = 0; i < scope.input.length; ++i) {
          var param = scope.input[i];
          if (param.options.length > 0 && param.multipleChoice) {
            scope.output[param.id] = (multiOutput[param.id] || []).join(',');
          }
        }
      });
    }
  };
});
