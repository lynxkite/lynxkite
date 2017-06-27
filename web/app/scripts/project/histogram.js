// Displays a histogram. The data comes from the "histogram-button" directive.
'use strict';

angular.module('biggraph').directive('histogram', function($timeout, util) {
  return {
    restrict: 'E',
    scope: { model: '=' },
    replace: false,
    templateUrl: 'scripts/project/histogram.html',
    link: function(scope) {
      function maxSize() {
        var max = 1;
        for (var i = 0; i < scope.model.sizes.length; ++i) {
          var s = scope.model.sizes[i];
          if (s > max) { max = s; }
        }
        return max;
      }

      function total() {
        var t = 0;
        for (var i = 0; i < scope.model.sizes.length; ++i) {
          t += scope.model.sizes[i];
        }
        return t;
      }

      function startLoading() {
        if (!scope.loading) {
          loadingAnimation();
        }
      }
      function loadingAnimation() {
        scope.loading = scope.model && !scope.model.$resolved;
        if (!scope.loading) { return; }
        scope.model.sizes = [];
        for (var i = 0; i < 20; ++i) {
          scope.model.sizes.push(Math.random());
        }
        scope.max = 1;
        scope.origMax = 1;
        $timeout(loadingAnimation, 200);
      }

      scope.$watch('model', update);
      scope.$watch('model.$resolved', update);

      function update() {
        var model = scope.model;
        if (!model) { return; }
        if (!model.$resolved) {
          startLoading();
          return;
        }
        scope.highlighted = undefined;  // Index of highlighted bar.
        scope.max = maxSize();
        scope.total = total();
        scope.origMax = scope.max;
        if (model.labelType === 'between') {
          var histoLabels = [];
          for (var j = 1; j < model.labels.length; ++j) {
            histoLabels[j - 1] = model.labels[j - 1] + '-' + model.labels[j];
          }
          scope.histoLabels = histoLabels;
        } else {
          scope.histoLabels = model.labels;
        }
      }

      scope.height = function(s) {
        return Math.max(0, Math.min(100, Math.floor(100 * s / scope.max))) + '%';
      };
      scope.clipped = function(s) {
        return scope.max < s;
      };
      scope.zoomable = function(s) {
        return !scope.loading && 0 < s && s < scope.origMax * 0.5;
      };
      scope.zoom = function(index) {
        if (!scope.zoomable(scope.model.sizes[index]) ||
            scope.highlighted === index) {
          // Unzoom.
          scope.highlighted = undefined;
          scope.max = scope.origMax;
        } else {
          // Zoom.
          scope.highlighted = index;
          scope.max = scope.model.sizes[index] * 2;
        }
      };
      scope.tooltipFor = function(index) {
        if (scope.loading) { return ''; }
        return scope.histoLabels[index] + ': ' + scope.model.sizes[index];
      };

      scope.reportError = function() {
        util.reportRequestError(scope.model, 'Histogram request failed.');
      };
    },
  };
});
