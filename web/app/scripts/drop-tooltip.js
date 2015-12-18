// A tooltip based on the "Drop" popup library.
'use strict';

angular.module('biggraph').directive('dropTooltip', function() {
  return {
    restrict: 'A',
    scope: {
      dropTooltipPosition: '@',
      dropTooltip: '@',
      dropTooltipEnable: '=',
    },
    link: function(scope, element) {
      /* global Drop */
      scope.createDrop = function() {
        return new Drop({
          target: element[0],
          content: scope.dropTooltip,
          position: scope.dropTooltipPosition || 'top center',
          openOn: 'hover',
          classes: 'drop-theme-tooltip',
          remove: true, // Remove from DOM when closing.
          tetherOptions: {
            // Keep within the page.
            constraints: [{
              to: 'window',
              attachment: 'together',
              pin: true,
            }],
          },
        });
      };
      var drop = scope.dropTooltipEnable === false ? undefined : scope.createDrop();
      scope.$watch('dropTooltip', function(tooltip) {
        if (drop) {
          drop.content.innerHTML = tooltip;
        }
      });
      scope.$watch('dropTooltipEnable', function(enabled) {
        if (enabled === false) {
          drop.destroy();
          drop = undefined;
        } else if (drop === undefined) {
          drop = scope.createDrop();
          drop.content.innerHTML = scope.dropTooltip;
        }
      });
      scope.$on('$destroy', function() {
        if (drop) {
          drop.destroy();
        }
      });
    },
  };
});
