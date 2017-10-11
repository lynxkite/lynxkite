// Presents the parameters of an operation. It picks the right presentation
// (text box, dropdown, etc) based on the parameter metadata.
'use strict';

angular.module('biggraph').directive('operationParameters', function(util) {
  return {
    restrict: 'E',
    scope: {
      box: '=',
      meta: '=',
      parameters: '=',
      parametricParameters: '=',
      onBlur: '&',
      busy: '=?',
    },
    templateUrl: 'scripts/operation/operation-parameters.html',
    link: function(scope, element) {
      element.on('focusout', function() { scope.onBlur(); });
      scope.fileUploads = { count: 0 };
      scope.$watch('fileUploads.count', function(count) {
        scope.busy = count !== 0;
      });

      scope.isParametric = function(param) {
        return param in scope.parametricParameters;
      };

      scope.toggleParametric = function(param) {
        if (param in scope.parametricParameters) {
          util.move(param, scope.parametricParameters, scope.parameters);
        } else {
          util.move(param, scope.parameters, scope.parametricParameters);
        }
        scope.onBlur();
      };

      scope.listParameters = {};
      util.deepWatch(scope, '[parameters, meta]', function() {
        var i, param;
        for (i = 0; i < scope.meta.parameters.length; ++i) {
          param = scope.meta.parameters[i];
          // Fill in default values.
          if (param.id in scope.parameters || param.id in scope.parametricParameters) {
            // Explicitly set.
          } else if (param.options.length === 0) {
            scope.parameters[param.id] = param.defaultValue;
          } else if (param.multipleChoice) {
            scope.parameters[param.id] = '';
          } else {
            scope.parameters[param.id] = param.options[0].id;
          }
        }

        for (i = 0; i < scope.meta.parameters.length; ++i) {
          param = scope.meta.parameters[i];
          // Translate between arrays and comma-separated strings for multiselects.
          if (param.options.length > 0 && param.multipleChoice) {
            var flat = scope.parameters[param.id];
            if (flat !== undefined && flat.length > 0) {
              scope.listParameters[param.id] = flat.split(',');
            } else {
              scope.listParameters[param.id] = [];
            }
          }
        }
      });

      util.deepWatch(scope, 'listParameters', function(listParameters) {
        for (var i = 0; i < scope.meta.parameters.length; ++i) {
          var param = scope.meta.parameters[i];
          if (param.options.length > 0 && param.multipleChoice) {
            scope.parameters[param.id] = (listParameters[param.id] || []).join(',');
          }
        }
      });

      scope.onLoad = function(editor) {
        editor.getSession().setTabSize(2);
        editor.renderer.setScrollMargin(7, 6);
        editor.setOptions({
          highlightActiveLine: false,
          maxLines: 50,
        });
        editor.commands.addCommand({
          name: 'blur',
          bindKey: {
            win: 'Ctrl-Enter',
            mac: 'Command-Enter',
            sender: 'editor|cli'
          },
          exec: function() {
            scope.$apply(function() {
              scope.onBlur();
            });
          }
        });
      };

      scope.isVisualizationParam = function(param) {
        return param.kind === 'visualization' && !scope.parametricParameters[param.id];
      };
    }
  };
});
