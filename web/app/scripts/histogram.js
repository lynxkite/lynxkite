'use strict';

angular.module('biggraph').directive('histogram', function() {
  return {
    restrict: 'E',
    scope: { model: '=' },
    replace: true,
    templateUrl: 'histogram.html',
    link: function(scope) {
      function maxSize() {
        var max = 1;
        for (var i = 0; i < scope.model.sizes.length; ++i) {
          var s = scope.model.sizes[i];
          if (s > max) { max = s; }
        }
        return max;
      }
      scope.$watch('model', function(model) {
        if (model === undefined || !model.$resolved) { return; }
        scope.highlighted = undefined;  // Index of highlighted bar.
        scope.max = maxSize();
        scope.origMax = scope.max;
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
        return Math.max(0, Math.min(100, Math.floor(100 * s / scope.max))) + '%';
      };
      scope.clipped = function(s) {
        return scope.max < s;
      };
      scope.zoomable = function(s) {
        return 0 < s && s < scope.origMax * 0.5;
      };
      scope.zoom = function(index) {
        if (!scope.zoomable(scope.model.sizes[index])) { return; }
        if (scope.highlighted === index) {
          // Unzoom.
          scope.highlighted = undefined;
          scope.max = scope.origMax;
        } else {
          // Zoom.
          scope.highlighted = index;
          scope.max = scope.model.sizes[index] * 2;
        }
      };
    },
  };
});
