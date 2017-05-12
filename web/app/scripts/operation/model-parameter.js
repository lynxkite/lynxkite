// Operation parameter for kind=model.
'use strict';

angular.module('biggraph').directive('modelParameter', function(util) {
  return {
    restrict: 'E',
    scope: {
      param: '=', // Parameters of the available models.
      editable: '=', // Whether this input is editable.
      modelJson: '=', // Input/output: Model configuration in JSON.
    },
    templateUrl: 'scripts/operation/model-parameter.html',
    link: function(scope) {
      scope.activeModel = undefined;
      // Feature name to attribute name. Matching names are added by default.
      scope.binding = {};
      for (var j = 0; j < scope.param.payload.attrs.length; ++j) {
        var id = scope.param.payload.attrs[j].id;
        scope.binding[id] = id;
      }

      // React to external changes to model.
      util.deepWatch(scope, 'modelJson', function(modelJson) {
        if (modelJson) {
          var modelParams = JSON.parse(modelJson);
          var models = scope.param.payload.models;
          scope.activeModel = undefined;
          for (var i = 0; i < models.length; ++i) {
            if (models[i].name === modelParams.modelName) {
              scope.activeModel = models[i];
            }
          }
          if (!scope.activeModel) {
            throw new Error('Could not find model "' + modelParams.modelName + '"');
          }
          for (i = 0; i < scope.activeModel.featureNames.length; ++i) {
            var feature = scope.activeModel.featureNames[i];
            scope.binding[feature] = modelParams.features[i];
          }
        }
      });

      util.deepWatch(scope, 'activeModel', updateModel);
      util.deepWatch(scope, 'binding', updateModel);

      function updateModel() {
        var modelParams = {};
        if (scope.activeModel) {
          var featureList = [];
          for (var i = 0; i < scope.activeModel.featureNames.length; ++i) {
            var feature = scope.activeModel.featureNames[i];
            featureList.push(scope.binding[feature]);
          }
          modelParams.modelName = scope.activeModel.name;
          modelParams.features = featureList;
        }
        scope.modelJson = JSON.stringify(modelParams);
      }
    },
  };
});
