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
        function embedPlot() {
          const plotElement = element.find('#plot-div')[0];
          if (scope.plotJSON === undefined || plotElement === undefined) {
            return;
          }
          /* global vega, vegaEmbed */
          vegaEmbed(plotElement, scope.plotJSON, {
            loader: vega.loader({ http: { headers: { 'X-Requested-With': 'XMLHttpRequest' } } }),
          });
        }

        scope.$watch('stateId', function(newValue, oldValue, scope) {
          scope.plotJSON = util.get('/ajax/getPlotOutput', {
            id: scope.stateId
          });
        });

        scope.$watch('plotJSON', embedPlot, true);
      },
    };
  });
