// The histogram button sends the histogram request and makes the results available.
// The "histogram" directive then shows the result.
'use strict';

angular.module('biggraph').directive('histogramButton', function(util) {
  return {
    restrict: 'E',
    scope: {
      attr: '=', side: '=',
      type: '@', // 'edge' or 'vertex'
      model: '=?', // The Ajax response is exported through this field.
      tsv: '=?', // A tab-separated table is exported through this field.
    },
    replace: true,
    templateUrl: 'histogram-button.html',
    link: function(scope) {
      function update() {
        if (!scope.show) {
          scope.model = undefined;
          return;
        }
        var q = {
          attributeId: scope.attr.id,
          vertexFilters: scope.side.nonEmptyVertexFilters(),
          edgeFilters: scope.side.nonEmptyEdgeFilters(),
          numBuckets: 20,
          axisOptions: scope.side.axisOptions(scope.type, scope.attr.title),
          edgeBundleId: scope.type === 'edge' ? scope.side.project.edgeBundle : '',
        };
        scope.model = util.get('/ajax/histo', q);
      }

      function updateTSV() {
        var model = scope.model;
        if (!model || !model.$resolved) {
          scope.tsv = '';
          return;
        }
        var tsv = '';
        // Header.
        if (model.labelType === 'between') {
          tsv += 'From\tTo\tCount\n';
        } else {
          tsv += 'Value\tCount\n';
        }
        // Data.
        for (var i = 0; i < model.sizes.length; ++i) {
          if (model.labelType === 'between') {
            tsv += model.labels[i] + '\t' + model.labels[i + 1];
          } else {
            tsv += model.labels[i];
          }
          tsv += '\t' + model.sizes[i] + '\n';
        }
        scope.tsv = tsv;
      }
      util.deepWatch(scope, 'side.state', update);
      scope.$watch('show', update);
      scope.$watch('model.$resolved', updateTSV);
    },
  };
});
