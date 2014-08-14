'use strict';

angular.module('biggraph').directive('histogram', function() {
  return {
    restrict: 'E',
    scope: { model: '=' },
    replace: true,
    templateUrl: 'histogram.html',
    link: function(scope) {
      scope.$watch('model', function(model) {
        if (model === undefined || !model.$resolved) { return; }
        var max = 1;
        for (var i = 0; i < model.sizes.length; ++i) {
          var s = model.sizes[i];
          if (s > max) { max = s; }
        }
        scope.max = max;
        if (model.labelType === 'between') {
          var histoLabels = [];
          for (var j = 1; j < model.labels.length; ++j) {
            histoLabels[j-1] = model.labels[j-1] + '-' + model.labels[j];
          }
          scope.histoLabels = histoLabels;
        } else {
          scope.histoLabels = model.labels;
        }
      }, true); // Watch contents of model.
      scope.height = function(s) {
        return Math.floor(100 * s / scope.max) + '%';
      };
    },
  };
});
