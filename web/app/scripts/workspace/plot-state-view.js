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
        scope.plot = util.get('/ajax/getPlotOutput', {
          id: scope.stateId
        });

        scope.plot.then(function() {
          scope.plotJSON = util.lazyFetchScalarValue(scope.plot.json, true);
          scope.embedSpec = {
            mode: "vega-lite",
          };
          scope.embedSpec.spec = JSON.parse(scope.plotJSON.value.string);
          /* global vg */
          console.log(scope.embedSpec);
          vg.embed("#vegaplot", scope.embedSpec, function() {});
        }, function() {
          console.log('plot error');
        });
      },
    };
  });
