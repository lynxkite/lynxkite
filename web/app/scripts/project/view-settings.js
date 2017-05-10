// Visualization settings shared code.
'use strict';

angular.module('biggraph').factory('ViewSettings', function() {
  return function(scope, element) {
    var that = this;
    this.drops = {};
    element.find('.token').each(function(i, e) {
      if (!e.id) { return; }
      var menu = element.find('#menu-' + e.id);
      if (!menu.length) { return; }
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
      that.drops['menu-' + e.id] = drop;
    });

    this.getDrop = function(e) {
      e = e.currentTarget;
      // Try to find a parent that is a drop-down menu. Give up after 20 steps.
      for (var i = 0; i < 20; ++i) {
        if (this.drops[e.id] !== undefined) {
          return this.drops[e.id];
        }
        e = e.parentNode;
      }
    };

    scope.saveVisualization = function(e) {
      scope.side.saveStateToBackend(
          scope.saveVisualizationName,
          function() { that.getDrop(e).close(); });
    };
  };
});
