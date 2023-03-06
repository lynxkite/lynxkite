'use strict';
import '../app';
import '../util/util';
import templateUrl from './plot-state-view.html?url';
import vega from 'vega';
import vegaEmbed from 'vega-embed';

// Viewer of a plot state.

angular.module('biggraph')
  .directive('plotStateView', ['environment', 'util', function(environment, util) {
    return {
      restrict: 'E',
      templateUrl,
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
