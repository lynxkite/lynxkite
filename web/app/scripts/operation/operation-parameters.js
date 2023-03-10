// Presents the parameters of an operation. It picks the right presentation
// (text box, dropdown, etc) based on the parameter metadata.
import '../app';
import '../util/util';
import templateUrl from './operation-parameters.html?url';

angular.module('biggraph').directive('operationParameters', ['util', function(util) {
  return {
    restrict: 'E',
    scope: {
      box: '=',
      meta: '=',
      parameters: '=',
      parametricParameters: '=',
      workspace: '=',
      wizard: '=',
      onBlur: '&',
    },
    templateUrl,
    link: function(scope, element) {
      element.on('focusout', function() { scope.onBlur(); });

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
        for (let i = 0; i < scope.meta.parameters.length; ++i) {
          const param = scope.meta.parameters[i];
          // Fill in default values.
          if (param.id in scope.parameters || param.id in scope.parametricParameters) {
            // Explicitly set.
          } else if (param.options.length === 0) {
            scope.parameters[param.id] = param.defaultValue;
          } else if (param.multipleChoice) {
            scope.parameters[param.id] = '';
          } else if (param.options.map(x => x.id).includes(param.defaultValue)) {
            scope.parameters[param.id] = param.defaultValue;
          } else {
            scope.parameters[param.id] = param.options[0].id;
          }

          // Translate between arrays and comma-separated strings for multiselects.
          if (param.options.length > 0 && param.multipleChoice) {
            const flat = scope.parameters[param.id];
            if (flat !== undefined && flat.length > 0) {
              scope.listParameters[param.id] = flat.split(',');
            } else {
              scope.listParameters[param.id] = [];
            }
          }
        }

        // Find unexpected parameters.
        const paramIds = Object.keys(scope.parameters);
        paramIds.sort();
        scope.unexpectedParameters = scope.unexpectedParameters || [];
        scope.unexpectedParameters.length = 0;
        for (let i = 0; i < paramIds.length; ++i) {
          const paramId = paramIds[i];
          let expected = false;
          for (let j = 0; j < scope.meta.parameters.length; ++j) {
            if (scope.meta.parameters[j].id === paramId) {
              expected = true;
              break;
            }
          }
          if (!expected) {
            scope.unexpectedParameters.push(paramId);
          }
        }
      });

      util.deepWatch(scope, 'listParameters', function(listParameters) {
        for (let i = 0; i < scope.meta.parameters.length; ++i) {
          const param = scope.meta.parameters[i];
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

      scope.removeParameter = function(paramId) {
        delete scope.parameters[paramId];
        scope.onBlur();
      };

      const expandedGroups = {};
      scope.visibleGroup = function(group) {
        if (group === '') {
          return true;
        } else {
          return expandedGroups[group];
        }
      };
      scope.expandGroup = function(g) {
        expandedGroups[g] = true;
      };
      scope.groups = function(params) {
        const gs = [];
        for (let p of params) {
          if (p.group && !gs.includes(p.group)) {
            gs.push(p.group);
          }
        }
        return gs;
      };
    }
  };
}]);
