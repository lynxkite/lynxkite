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
        for (var i = 0; i < model.vertices.length; ++i) {
          var v = model.vertices[i];
          if (v.size > max) { max = v.size; }
        }
        scope.max = max;
        if (model.xLabelType === 'between') {
          var histoLabels = [];
          for (var j = 1; j < model.xLabels.length; ++j) {
            histoLabels[j-1] = model.xLabels[j-1] + '-' + model.xLabels[j];
          }
          scope.histoLabels = histoLabels;
        } else {
          scope.histoLabels = model.xLabels;
        }
      }, true); // Watch contents of model.
      scope.height = function(v) {
        return Math.floor(100 * v.size / scope.max) + '%';
      };
    },
  };
});
