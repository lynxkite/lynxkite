// Presents the parameters of an operation. It picks the right presentation
// (text box, dropdown, etc) based on the parameter metadata.
'use strict';

angular.module('biggraph').directive('operationParameters', function(util) {
  return {
    restrict: 'E',
    scope: { op: '=', output: '=', busy: '=?' },
    templateUrl: 'operation-parameters.html',
    link: function(scope) {
      scope.fileUploads = { count: 0 };
      scope.$watch('fileUploads.count', function(count) {
        scope.busy = count !== 0;
      });
      // Translate between arrays and comma-separated strings for multiselects.
      scope.multiOutput = {};
      util.deepWatch(scope, 'output', function(output) {
        for (var i = 0; i < scope.op.parameters.length; ++i) {
          var param = scope.op.parameters[i];
          if (param.options.length > 0 && param.multipleChoice) {
            var flat = output[param.id];
            if (flat !== undefined && flat.length > 0) {
              scope.multiOutput[param.id] = flat.split(',');
            } else {
              scope.multiOutput[param.id] = [];
            }
          }
        }
      });
      util.deepWatch(scope, 'multiOutput', function(multiOutput) {
        for (var i = 0; i < scope.op.parameters.length; ++i) {
          var param = scope.op.parameters[i];
          if (param.options.length > 0 && param.multipleChoice) {
            scope.output[param.id] = (multiOutput[param.id] || []).join(',');
          }
        }
      });
    }
  };
});
