// UI for defining workspace parameters. In other words, the parameters of a custom operation.
'use strict';

angular.module('biggraph').directive('parametersParameter', function(util) {
  return {
    restrict: 'E',
    scope: {
      model: '=',
    },
    templateUrl: 'parameters-parameter.html',
    link: function(scope) {
      scope.validKinds = [
        'text',
        'boolean',
        'code',
        'vertexattribute',
        'edgeattribute',
        'segmentation',
      ];

      scope.$watch('model', function(model) {
        scope.parameters = JSON.parse(model);
        console.log('w', scope.parameters);
      });
      util.deepWatch(scope, 'parameters', function(parameters) {
        scope.model = JSON.stringify(parameters);
      });

      scope.add = function() {
        scope.parameters.push({ kind: 'text', id: '', defaultValue: '' });
        console.log(scope.parameters);
      };

      scope.discard = function(index) {
        scope.parameters.splice(index, 1);
        console.log(scope.parameters);
      };
    },
  };
});
