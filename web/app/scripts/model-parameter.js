// Operation parameter for kind=model.
'use strict';

angular.module('biggraph').directive('modelParameter', function(util) {
  return {
    restrict: 'E',
    scope: {
      param: '=',
      editable: '=',
      model: '=',
    },
    templateUrl: 'model-parameter.html',
    link: function(scope) {
      scope.binding = [];
      util.deepWatch(scope, 'binding', function(binding) {
        if (scope.activeModel && binding) {
          var featureList = [scope.activeModel.name];
          for (var i = 0; i  < scope.activeModel.featureNames.length; ++i) {
            featureList.push(binding[i]);
          }
          scope.model = featureList.join(',');
        }
      });
    },
  };
});
