'use strict';

// A button that pops up a textbox with the specified contents selected and ready to be copied.
angular.module('biggraph').directive('copyBox', function() {
  return {
    restrict: 'E',
    scope: { data: '@', description: '@' },
    templateUrl: 'copy-box.html',
    link: function(scope, element) {
      scope.$watch('show', function(show) {
        if (show) {
          // Need to select the contents a moment later, so that mouseup/keyup events do not
          // undo the selection.
          window.setTimeout(function() {
            element.find('textarea').select();
          });
        }
      });
    },
  };
});
