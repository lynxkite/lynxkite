// A button that pops up a textbox with the specified contents selected and ready to be copied.
'use strict';

angular.module('biggraph').directive('copyBox', function() {
  return {
    restrict: 'E',
    scope: { data: '@', description: '@' },
    templateUrl: 'copy-box.html',
    link: function(scope, element) {
      ZeroClipboard.config({ swfPath: 'zeroclipboard/dist/ZeroClipboard.swf' });
      var client = new ZeroClipboard(element.find('.clicky'));
      client.on('error', function(event) {
        // No flash, no copy.
        console.log('Copy to clipboard is disabled: ' + event.name);
        element.empty();
      });
      scope.$on('$destroy', function() {
        client.destroy();
      });
    },
  };
});
