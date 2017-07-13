// Editor of operation parameters.
'use strict';

angular.module('biggraph')
  .directive('opEditor', function(util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/op-editor.html',
      scope: {
        box: '=?',
        boxMeta: '=',
        parameters: '=',
        parametricParameters: '=',
        halfSize: '=?',
        onBlurUp: '&?onBlur',
      },
      link: function(scope) {
        // The external parameters are not directly edited, just updated on "blur".
        util.deepWatch(scope, '[boxMeta, parameters, parametricParameters]', function() {
          var e = externalToInternal();
          var i = internal();
          if (!angular.equals(e, i)) {
            scope.plainParamValues = e.plainParamValues;
            scope.parametricParamValues = e.parametricParamValues;
            scope.parametricFlags = e.parametricFlags;
          }
        });

        function externalToInternal() {
          if (!scope.boxMeta) {
            return {
              plainParamValues: {},
              parametricParamValues: {},
              parametricFlags: {},
            };
          }
          // Make a copy of the parameter values.
          var paramValues = angular.copy(scope.parameters);
          var parametricParamValues = angular.copy(scope.parametricParameters);
          var parametricFlags = {};

          // Copy defaults for unset parameters.
          for (var i = 0; i < scope.boxMeta.parameters.length; ++i) {
            var p = scope.boxMeta.parameters[i];
            var id = p.id;
            if (paramValues[id] !== undefined ||
                parametricParamValues[id] !== undefined) {
              // Parameter p is not unset
            } else if (p.options.length === 0) {
              paramValues[id] = p.defaultValue;
            } else if (p.multipleChoice) {
              paramValues[id] = '';
            } else {
              paramValues[id] = p.options[0].id;
            }
          }

          // Re-establish parametric flags.
          for (var k = 0; k < scope.boxMeta.parameters.length; ++k) {
            id = scope.boxMeta.parameters[k].id;
            if (parametricParamValues[id] !== undefined) {
              parametricFlags[id] = true;
            } else {
              parametricFlags[id] = false;
            }
          }
          return {
            plainParamValues: paramValues,
            parametricParamValues: parametricParamValues,
            parametricFlags: parametricFlags,
          };
        }

        function internal() {
          return {
            plainParamValues: scope.plainParamValues,
            parametricParamValues: scope.parametricParamValues,
            parametricFlags: scope.parametricFlags,
          };
        }

        scope.onBlur = function() {
          var e = externalToInternal();
          var i = internal();
          if (!angular.equals(e, i)) {
            scope.parameters = angular.merge(scope.parameters, scope.plainParamValues);
            scope.parametricParameters =
              angular.merge(scope.parametricParameters, scope.parametricParamValues);
            if (scope.onBlurUp) {
              scope.onBlurUp();
            }
          }
        };
      },
    };
  });
