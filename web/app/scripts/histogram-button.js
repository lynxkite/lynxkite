'use strict';

angular.module('biggraph').directive('histogramButton', function($resource) {
  return {
    restrict: 'E',
    scope: { attr: '=', side: '=', histogram: '=model' },
    replace: true,
    templateUrl: 'histogram-button.html',
    link: function(scope) {
      var vertexDiag = $resource('/ajax/vertexDiag', {}, { get: { method: 'GET', cache: true } });
      function update() {
        if (!scope.show) {
          scope.histogram = undefined;
          return;
        }
        var filters = [];
        var state = scope.side.state;
        var data = scope.side.data;
        for (var filteredAttr in state.filters) {
          if (state.filters[filteredAttr] !== '') {
            filters.push({ attributeId: filteredAttr, valueSpec: state.filters[filteredAttr] });
          }
        }
        var q = {
          vertexSetId: data.id,
          filters: filters,
          mode: 'bucketed',
          xBucketingAttributeId: scope.attr.id,
          xNumBuckets: 20,
          yBucketingAttributeId: '',
          yNumBuckets: 1,
          // Unused.
          centralVertexId: '',
          sampleSmearEdgeBundleId: '',
          radius: 0,
          labelAttributeId: '',
          sizeAttributeId: '',
        };
        scope.histogram = vertexDiag.get({q: q});
      }
      angular.deepWatch(scope, 'side', update);
      scope.$watch('show', update);
    },
  };
});
