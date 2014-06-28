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
          if (v.count > max) {
            max = v.count;
          }
        }
        scope.max = max;
      }, true); // Watch contents of model.
      scope.height = function(v) {
        return Math.floor(100 * v.count / scope.max) + '%';
      };
    },
  };
});
