// A tooltip based on the "Drop" popup library.
'use strict';

angular.module('biggraph')
.service('dropTooltipConfig', function() {
  this.enabled = true;
})
.directive('dropTooltip', function(dropTooltipConfig) {
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
        if (!dropTooltipConfig.enabled) {
          return;
        }
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
      scope.$watchGroup(['dropTooltip', 'dropTooltipEnable', 'dropTooltipPosition'], updateTooltip);
      scope.$on('$destroy', function() {
        if (drop) {
          drop.destroy();
          drop = undefined;
        }
      });
    },
  };
});
