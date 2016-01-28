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
      scope.$watch('activeModel', function(activeModel) {
        scope.binding = [];
        if (activeModel) {
          for (var i = 0; i < activeModel.featureNames.length; ++i) {
            for (var j in scope.param.payload.attrs) {
              var attr = scope.param.payload.attrs[j];
              if (attr.id === activeModel.featureNames[i]) {
                scope.binding[i] = attr.id;
              }
            }
          }
        }
      });
      util.deepWatch(scope, 'binding', function(binding) {
        if (scope.activeModel && binding) {
          var featureList = [];
          for (var i = 0; i < scope.activeModel.featureNames.length; ++i) {
            featureList.push(binding[i]);
          }
          var modelParams = {};
          modelParams.modelName = scope.activeModel.name;
          modelParams.features = featureList;
          scope.model = JSON.stringify(modelParams);
        }
      });
    },
  };
});
