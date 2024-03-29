// UI for defining workspace parameters. In other words, the parameters of a custom operation.
import '../app';
import '../util/util';
import templateUrl from './parameters-parameter.html?url';

angular.module('biggraph').directive('parametersParameter', ['util', function(util) {
  return {
    restrict: 'E',
    scope: {
      model: '=',
    },
    templateUrl,
    link: function(scope) {
      scope.validKinds = util.globals.workspaceParameterKinds;

      scope.$watch('model', function(model) {
        scope.parameters = JSON.parse(model);
      });
      util.deepWatch(scope, 'parameters', function(parameters) {
        scope.model = JSON.stringify(parameters);
      });

      scope.add = function() {
        scope.parameters.push({ kind: 'text', id: '', defaultValue: '' });
        setTimeout(() => angular.element('.param-id').last()[0].focus());
      };

      scope.discard = function(index) {
        scope.parameters.splice(index, 1);
      };
    },
  };
}]);
