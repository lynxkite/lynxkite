'use strict';

// Viewer of a plot state.

angular.module('biggraph')
  .directive('plotStateView', function(util) {
    return {
      restrict: 'E',
      templateUrl: 'scripts/workspace/plot-state-view.html',
      scope: {
        stateId: '=',
        popupModel: '=',
      },
      link: function(scope, element) {
        scope.updatePlotSpec = function () {
          scope.embedSpec.spec = JSON.parse(scope.plotJSON.value.string);
        };

        scope.embedPlot = function () {
          scope.updatePlotSpec();
          const plotElement = element.find('#plot-div')[0];
          /* global vg */
          vg.embed(plotElement, scope.embedSpec, function() {});
        };

        scope.$watch('stateId', function(newValue, oldValue, scope) {
          scope.embedSpec = {
            mode: 'vega-lite',
            actions: false,
            renderer: 'svg',
          };

          scope.plot = util.get('/ajax/getPlotOutput', {
            id: scope.stateId
          });
          scope.plot.then(
            function() {
              scope.plotJSON = util.lazyFetchScalarValue(scope.plot.json, true);
            }, function() {}
          );
        });

        scope.$watch('plotJSON', function() {
          if (scope.plotJSON && scope.plotJSON.value && scope.plotJSON.value.string) {
            scope.embedPlot();
          }
        }, true);

      },
    };
  });
