'use strict';

angular.module('biggraph').directive('sampledViewSettings', function() {
  return {
    scope: { side: '=' },
    restrict: 'E',
    templateUrl: 'sampled-view-settings.html',
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

      scope.saveVisualization = function(e) {
        scope.side.saveStateToBackend(
            scope.saveVisualizationName,
            function() { getDrop(e).close(); });
      };
    },
  };
});
