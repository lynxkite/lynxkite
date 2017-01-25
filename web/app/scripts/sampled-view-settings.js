'use strict';

angular.module('biggraph').directive('sampledViewSettings', function($timeout) {
  return {
    scope: { side: '=' },
    restrict: 'E',
    templateUrl: 'sampled-view-settings.html',
    link: function(scope, element) {
      var show = {};
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

      scope.showDisplay = function() {
        return show.display || scope.side.state.display !== 'svg'; };
      scope.showLayout = function() {
        return show.layout || scope.side.state.animate.style !== 'neutral'; };
      scope.showAttraction = function() {
        return show.attraction || scope.side.state.animate.labelAttraction.toString() !== '0'; };
      scope.showRadius = function() {
        return (
          scope.side.project.edgeBundle &&
          (show.radius || scope.side.state.sampleRadius.toString() !== '1'));
      };

      scope.addSetting = function(e, setting) {
        show[setting] = true;
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
              getDrop(e).close();
            });
      };
    },
  };
});
