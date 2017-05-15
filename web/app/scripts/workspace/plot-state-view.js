'use strict';

// Viewer of a plot state.

angular.module('biggraph')
  .directive('plotStateView', function(util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/plot-state-view.html',
      scope: {
        stateId: '=',
      },
      link: function(scope) {
        scope.inProgress = 0;
        scope.plotDivId = 'vegaplot-' + scope.stateId;

        scope.showPlot = function() {
          scope.plot = util.get('/ajax/getPlotOutput', {
            id: scope.stateId
          });
          scope.plot.then(function() {
            scope.inProgress = 1;
            scope.plotJSON = util.lazyFetchScalarValue(scope.plot.json, true);
            scope.inProgress = 0;
            scope.embedSpec = {
              mode: "vega-lite",
            };
            scope.embedSpec.spec = JSON.parse(scope.plotJSON.value.string);
            /* global vg */
            vg.embed('#' + scope.plotDivId, scope.embedSpec, function() {});
          }, function() {
            console.log('plot error');
          });
        };

        scope.onload = scope.showPlot();
      },
    };
  });
