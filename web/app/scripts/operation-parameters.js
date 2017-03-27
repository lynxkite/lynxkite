// Presents the parameters of an operation. It picks the right presentation
// (text box, dropdown, etc) based on the parameter metadata.
'use strict';

angular.module('biggraph').directive('operationParameters', function(util) {
  return {
    restrict: 'E',
    scope: { params: '=', output: '=', busy: '=?', editable: '=', parametric: '=' },
    templateUrl: 'operation-parameters.html',
    link: function(scope) {
      scope.fileUploads = { count: 0 };
      scope.$watch('fileUploads.count', function(count) {
        scope.busy = count !== 0;
      });

      // Translate between arrays and comma-separated strings for multiselects.
      scope.multiOutput = {};
      util.deepWatch(scope, 'output', function(output) {
        for (var i = 0; i < scope.params.length; ++i) {
          var param = scope.params[i];
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
        for (var i = 0; i < scope.params.length; ++i) {
          var param = scope.params[i];
          if (param.options.length > 0 && param.multipleChoice) {
            scope.output[param.id] = (multiOutput[param.id] || []).join(',');
          }
        }
      });

      scope.onLoad = function(editor) {
        editor.getSession().setTabSize(2);
        editor.renderer.setScrollMargin(3, 3, 3, 3);
        editor.setOptions({
          highlightActiveLine: false,
          maxLines: 50,
        });
      };
    }
  };
});
