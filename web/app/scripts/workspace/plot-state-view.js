import '../app';
import '../util/util';
import templateUrl from './plot-state-view.html?url';

// Viewer of a plot state.

angular.module('biggraph').directive('plotStateView', ['environment', 'util', function(environment, util) {
  return {
    restrict: 'E',
    templateUrl,
    scope: {
      stateId: '=',
      popupModel: '=',
    },
    link: function(scope, element) {
      async function embedPlot() {
        const vega = await import('vega');
        const vegaEmbed = (await import('vega-embed')).default;
        const plotElement = element.find('#plot-div')[0];
        if (scope.plotJSON === undefined || plotElement === undefined) {
          return;
        }
        vegaEmbed(plotElement, scope.plotJSON, {
          ...environment.vegaConfig,
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
}]);
