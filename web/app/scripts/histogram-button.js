'use strict';

angular.module('biggraph').directive('histogramButton', function(util) {
  return {
    restrict: 'E',
    scope: { attr: '=', side: '=', histogram: '=model' },
    replace: true,
    templateUrl: 'histogram-button.html',
    link: function(scope) {
      function update() {
        if (!scope.show) {
          scope.histogram = undefined;
          return;
        }
        var q = {
          attributeId: scope.attr.id,
          vertexFilters: scope.side.nonEmptyFilters(),
          numBuckets: 20,
        };
        scope.histogram = util.get('/ajax/histo', q);
      }
      util.deepWatch(scope, 'side.state', update);
      scope.$watch('show', update);
    },
  };
});
