'use strict';

angular.module('biggraph').directive('bucketedViewSettings', function($timeout) {
  return {
    scope: { side: '=' },
    restrict: 'E',
    templateUrl: 'bucketed-view-settings.html',
    link: function(scope, element) {
      var drops = {};

      element.find('.entity').each(function(i, e) {
        if (!e.id) { return; }
        var menu = element.find('#menu-' + e.id);
        /* global Drop */
        var drop = new Drop({
          target: e,
          content: menu[0],
          openOn: 'click',
          classes: 'drop-theme-menu',
          remove: true,
          position: 'bottom center',
          tetherOptions: {
            constraints: [{ to: 'window', attachment: 'together', pin: true, }],
          },
        });
        drop.on('close', function() { $timeout(function() {
          scope.savingVisualization = false;
        }); });
        drops['menu-' + e.id] = drop;
      });

      function getDrop(e) {
        e = e.currentTarget;
        for (var i = 0; i < 20; ++i) {
          if (drops[e.id] !== undefined) {
            return drops[e.id];
          }
          e = e.parentNode;
        }
      }

      scope.closeDrop = function(e) {
        getDrop(e).close();
      };

      scope.startSavingVisualization = function(e) {
        scope.savingVisualization = true;
        $timeout(function() {
          angular.element(getDrop(e).content).find('#save-visualization-name').focus();
        });
      };

      scope.saveVisualization = function(e) {
        scope.side.saveStateToBackend(
            scope.saveVisualizationName,
            function() {
              scope.savingVisualization = false;
              scope.closeDrop(e);
            });
      };
    },
  };
});
