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
      var drop;
      var defaultPosition = 'bottom center';
      scope.createDrop = function() {
        /* global Drop */
        return new Drop({
          target: element[0],
          content: scope.dropTooltip,
          position: scope.dropTooltipPosition || defaultPosition,
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
      function updateTooltip() {
        var enabled = scope.dropTooltipEnable ||
          scope.dropTooltipEnable === undefined;
        if (!enabled || !scope.dropTooltip) {
          if (drop) {
            drop.destroy();
            drop = undefined;
            return;
          }
        } else {
          if (!drop) {
            drop = scope.createDrop();
          } else {
            drop.content.innerHTML = scope.dropTooltip;
            drop.position = scope.dropTooltipPosition || defaultPosition;
          }
        }
      }
      scope.$watch('dropTooltip', updateTooltip);
      scope.$watch('dropTooltipEnable', updateTooltip);
      scope.$watch('dropTooltipPosition', updateTooltip);
      scope.$on('$destroy', function() {
        if (drop) {
          drop.destroy();
        }
      });
    },
  };
});
