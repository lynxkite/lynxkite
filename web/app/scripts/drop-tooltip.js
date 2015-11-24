// A tooltip based on the "Drop" popup library.
'use strict';

angular.module('biggraph').directive('dropTooltip', function() {
  return {
    restrict: 'A',
    scope: {
      dropTooltip: '@',
    },
    link: function(scope, element) {
      /* global Drop */
      var drop = new Drop({
        target: element[0],
        content: scope.dropTooltip,
        position: 'bottom center',
        openOn: 'hover',
        classes: 'drop-theme-tooltip',
        remove: true, // Remove from DOM when closing.
        tetherOptions: {
          // Keep within the page.
          constraints: [{
            to: 'scrollParent',
            pin: true,
          }],
        },
      });
      scope.$watch('dropTooltip', function(tooltip) {
        drop.content.innerHTML = tooltip;
      });
      scope.$on('$destroy', function() {
        drop.destroy();
      });
    },
  };
});
