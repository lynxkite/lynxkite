// Operation parameter for kind=model.
import '../app';
import '../util/util';
import templateUrl from './model-parameter.html?url';

angular.module('biggraph').directive('modelParameter', ['util', function(util) {
  return {
    restrict: 'E',
    scope: {
      param: '=', // Parameters of the available models.
      modelJson: '=', // Input/output: Model configuration in JSON.
      onBlur: '&', // Function to call on changes.
    },
    templateUrl,
    link: function(scope) {
      scope.activeModel = undefined;
      // Feature name to attribute name. Matching names are added by default.
      scope.binding = {};
      for (let j = 0; j < scope.param.payload.attrs.length; ++j) {
        const id = scope.param.payload.attrs[j].id;
        scope.binding[id] = id;
      }
      scope.getParamsWithMatchingType = function(type) {
        return scope.param.payload.attrs.filter(function(attr, index) {
          return scope.param.payload.attrTypes[index] === type;
        });
      };

      // React to external changes to model.
      util.deepWatch(scope, 'modelJson', function(modelJson) {
        if (modelJson) {
          const modelParams = JSON.parse(modelJson);
          const models = scope.param.payload.models;
          scope.activeModel = undefined;
          for (let i = 0; i < models.length; ++i) {
            if (models[i].name === modelParams.modelName) {
              scope.activeModel = models[i];
            }
          }
          if (!scope.activeModel) {
            throw new Error('Could not find model "' + modelParams.modelName + '"');
          }
          for (let i = 0; i < scope.activeModel.featureNames.length; ++i) {
            const feature = scope.activeModel.featureNames[i];
            scope.binding[feature] = modelParams.features[i];
          }
        }
      });

      util.deepWatch(scope, 'activeModel', updateModel);
      util.deepWatch(scope, 'binding', updateModel);

      function updateModel() {
        const modelParams = {};
        if (scope.activeModel) {
          const featureList = [];
          for (let i = 0; i < scope.activeModel.featureNames.length; ++i) {
            const feature = scope.activeModel.featureNames[i];
            featureList.push(scope.binding[feature]);
          }
          modelParams.modelName = scope.activeModel.name;
          modelParams.features = featureList;
        }
        scope.modelJson = JSON.stringify(modelParams);
      }
    },
  };
}]);
