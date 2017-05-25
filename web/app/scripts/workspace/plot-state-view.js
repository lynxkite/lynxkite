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
      link: function(scope) {
        // We leave some empty space.
        scope.getPlotWidth = function () {
          return scope.popupModel.width - 50;
        };

        // We leave some more empty space for the header.
        scope.getPlotHeight = function () {
          return scope.popupModel.height - 100;
        };

        scope.updatePlotSpec = function () {
          scope.embedSpec.spec = JSON.parse(scope.plotJSON.value.string);
          // After lazyFetchScalarValue the stateId can be changed.
          scope.plotDivId = 'vegaplot-' + scope.stateId;
        };

        // Vega-embed can be configured using `width` and `height` parameters, but
        // unfortunately the size of the axes and the legend is not included in these
        // parameters. So if we want to control the "outer" or "real" size of the plot,
        // we need extra steps:
        //
        // We embed a plot into a hidden DIV to get its real size.
        // From the size of the hidden plot we can compute the difference
        // between the desired and the actual size. If the actual size is larger than
        // the desired size, we will adjust the parameters of the embed call with
        // the difference.
        scope.computeSizeDiff = function() {
          scope.updatePlotSpec();
          // Desired plot size
          scope.embedSpec.spec.width = scope.getPlotWidth();
          scope.embedSpec.spec.height = scope.getPlotHeight();
          /* global vg */
          vg.embed('#' + scope.plotDivId + '-hidden', scope.embedSpec, function() {
            var svg = angular.element('#' + scope.plotDivId + '-hidden .vega svg')[0];
            var w = svg.attributes['width'].value;
            var h = svg.attributes['height'].value;
            // The assumption is that the difference is constant, not linear.
            var diffX = scope.getPlotWidth() - w;
            var diffY = scope.getPlotHeight() - h;
            scope.diffX = diffX < 0 ? diffX : 0;
            scope.diffY = diffY < 0 ? diffY : 0;
          });
        };

        scope.embedPlot = function () {
          scope.updatePlotSpec();
          if (scope.diffX !== undefined && scope.diffY !== undefined) {
            scope.embedSpec.spec.width = scope.getPlotWidth() + scope.diffX;
            scope.embedSpec.spec.height = scope.getPlotHeight() + scope.diffY;
            /* global vg */
            vg.embed('#' + scope.plotDivId, scope.embedSpec, function() {});
          }
        };

        scope.$watch('stateId', function(newValue, oldValue, scope) {
          scope.plotDivId = 'vegaplot-' + scope.stateId;
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
        }, true);

        scope.$watchGroup([
          'popupModel.width', 'popupModel.height',
          'plotJSON.value.string',
          'diffX', 'diffY'],
          function() {
            if (scope.plotJSON && scope.plotJSON.value && scope.plotJSON.value.string) {
              scope.embedPlot();
            }
          }
         );

        scope.$watch('embedSpec.spec.description', function(newValue, oldValue, scope) {
          // Refresh title after embedding the plot
          if (scope.embedSpec.spec && scope.embedSpec.spec.description) {
            scope.title = scope.embedSpec.spec.description;
          }
        }, true);

        scope.$watch('plotJSON', function(newValue, oldValue, scope) {
          if (scope.plotJSON && scope.plotJSON.value && scope.plotJSON.value.string) {
            scope.diffX = undefined;
            scope.diffY = undefined;
            scope.computeSizeDiff();
          }
        }, true);

      },
    };
  });
