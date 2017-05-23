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
        // We need some additional space for the axes and the legend.
        scope.getPlotWidth = function () {
          return scope.popupModel.width - 150;
        };

        scope.getPlotHeight = function () {
          return scope.popupModel.height - 150;
        };

        scope.$watch('stateId', function(newValue, oldValue, scope) {
          scope.plotDivId = 'vegaplot-' + scope.stateId;
          scope.title = 'unnamed';
          scope.plotWidth = scope.getPlotWidth();
          scope.plotHeight = scope.getPlotHeight();

          scope.plot = util.get('/ajax/getPlotOutput', {
            id: scope.stateId
          });
          scope.plot.then(function() {
            scope.plotJSON = util.lazyFetchScalarValue(scope.plot.json, true);
          }, function() {}
          );
        }, true);

        scope.$watch('popupModel.width', function(newValue, oldValue, scope) {
          scope.plotWidth = scope.getPlotWidth();
        }, true);

        scope.$watch('popupModel.height', function(newValue, oldValue, scope) {
          scope.plotHeight = scope.getPlotHeight();
        }, true);

        scope.$watch('embedSpec.spec.description', function(newValue, oldValue, scope) {
          if (newValue) {
            scope.title = newValue;
          }
        }, true);

        scope.embedPlot = function (width, height) {
          scope.embedSpec = {
            mode: "vega-lite",
            actions: false,
          };
          scope.embedSpec.spec = JSON.parse(scope.plotJSON.value.string);
          if (width && height) {
            scope.embedSpec.spec.width = width;
            scope.embedSpec.spec.height = height;
          }
          // After lazyFetchScalarValue the stateId can be changed.
          scope.plotDivId = 'vegaplot-' + scope.stateId;
          /* global vg */
          vg.embed('#' + scope.plotDivId, scope.embedSpec, function() {});
        };

        scope.resizePlot = function() {
          if (scope.plotJSON && scope.plotJSON.value && scope.plotJSON.value.string) {
            scope.embedPlot(scope.plotWidth, scope.plotHeight);
          }
        };

        scope.$watch('plotJSON', function(newValue, oldValue, scope) {
          if (scope.plotJSON && scope.plotJSON.value && scope.plotJSON.value.string) {
            scope.embedPlot();
          }
        }, true);
      },
    };
  });
