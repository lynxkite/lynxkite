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
        // This will change when the window is resized.
        scope.width = 350;
        scope.height = 320;
        scope.$watch('stateId', function(newValue, oldValue, scope) {
          scope.plotDivId = 'vegaplot-' + scope.stateId;
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
            actions: false,
          };
          scope.embedSpec.spec = JSON.parse(scope.plotJSON.value.string);
          scope.embedSpec.spec.width = scope.width;
          scope.embedSpec.spec.height = scope.height;
          // After lazyFetchScalarValue the stateId can be changed.
          scope.plotDivId = 'vegaplot-' + scope.stateId;
          /* global vg */
          vg.embed('#' + scope.plotDivId, scope.embedSpec, function() {});
        }, true);
      },
    };
  });
