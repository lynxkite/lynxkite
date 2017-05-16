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
        scope.plotDivId = 'vegaplot-' + scope.stateId;
//        console.log('Stateid 1: ', scope.plotDivId);
        scope.rendered = 0;

        console.log('Plot scalar computation started');
        scope.plot = util.get('/ajax/getPlotOutput', {
          id: scope.stateId
        });
        scope.plot.then(function() {
          scope.plotJSON = util.lazyFetchScalarValue(scope.plot.json, true);
        }, function() {
          console.log('plot error');
        });
        console.log('Plot scalar computed');

        scope.showPlot = function() {
          console.log('Plotting is started.');
          scope.embedSpec = {
            mode: "vega-lite",
          };
//          console.log('Before parse JSON: ', scope.plotJSON.value.string);
          scope.embedSpec.spec = JSON.parse(scope.plotJSON.value.string);

          // After lazyFetchScalarValue the stateId can be changed.
          scope.plotDivId = 'vegaplot-' + scope.stateId;
//        console.log('Stateid 2: ', scope.plotDivId);
          if (scope.rendered === 0) {
            /* global vg */
            vg.embed('#' + scope.plotDivId, scope.embedSpec, function() {});
            scope.rendered = 1;
          }
          return 'The plot is rendered.';
        };
      },
    };
  });
