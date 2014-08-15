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
        var filters = [];
        var state = scope.side.state;
        for (var filteredAttr in state.filters) {
          if (state.filters[filteredAttr] !== '') {
            filters.push({ attributeId: filteredAttr, valueSpec: state.filters[filteredAttr] });
          }
        }
        var q = {
          attributeId: scope.attr.id,
          vertexFilters: filters,
          numBuckets: 20,
        };
        scope.histogram = util.get('/ajax/histo', q);
      }
      util.deepWatch(scope, 'side', update);
      scope.$watch('show', update);
    },
  };
});
