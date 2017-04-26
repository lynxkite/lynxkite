// A button that pops up a textbox with the specified contents selected and ready to be copied.
'use strict';

angular.module('biggraph').directive('copyBox', function(util) {
  return {
    restrict: 'E',
    scope: { data: '@', description: '@' },
    templateUrl: 'copy-box.html',
    link: function(scope, element) {
      /* global Clipboard */
      var client = new Clipboard(element.find('.clicky')[0]);
      client.on('error', function(event) {
        /* eslint-disable no-console */
        console.log('Copy to clipboard is disabled:', event);
        util.error('Could not copy to clipboard: ' + scope.data);
      });
      scope.$on('$destroy', function() {
        client.destroy();
      });
    },
  };
});
