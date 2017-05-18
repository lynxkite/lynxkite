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
        scope.$watch('stateId', function(newValue, oldValue, scope) {
          scope.plotDivId = 'vegaplot-' + scope.stateId;
          scope.rendered = 0;
          scope.title = 'unnamed';

          scope.plot = util.get('/ajax/getPlotOutput', {
            id: scope.stateId
          });
          scope.plot.then(function() {
            scope.plotJSON = util.lazyFetchScalarValue(scope.plot.json, true);
          }, function() {

          });
        }, true);

        scope.$watch('embedSpec.spec.description', function(newValue, oldValue, scope) {
          if (newValue) {
            scope.title = newValue;
          }
        }, true);

        scope.$watch('plotJSON.value.string', function(newValue, oldValue, scope) {
          scope.embedSpec = {
            mode: "vega-lite",
          };
          scope.embedSpec.spec = JSON.parse(scope.plotJSON.value.string);

          // After lazyFetchScalarValue the stateId can be changed.
          scope.plotDivId = 'vegaplot-' + scope.stateId;
          if (scope.rendered === 0) {
            /* global vg */
            vg.embed('#' + scope.plotDivId, scope.embedSpec, function() {});
            scope.rendered = 1;
          }
        }, true);

      },
    };
  });
