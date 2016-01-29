// Operation parameter for kind=model.
'use strict';

angular.module('biggraph').directive('modelParameter', function(util) {
  return {
    restrict: 'E',
    scope: {
      param: '=', // Parameters of the available models.
      editable: '=', // Whether this input is editable.
      model: '=', // Output: arguments to run the model with.
      op: '=?', // The id of the operation of this parameter.
    },
    templateUrl: 'model-parameter.html',
    link: function(scope) {
      scope.$watch('activeModel', function(activeModel) {
        scope.binding = [];
        if (activeModel) {
          for (var i = 0; i < activeModel.featureNames.length; ++i) {
            for (var j = 0; j < scope.param.payload.attrs.length; ++j) {
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
