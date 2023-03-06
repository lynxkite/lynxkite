// A button that pops up a textbox with the specified contents selected and ready to be copied.
'use strict';
import ClipboardJS from 'clipboard';
import '../app';
import './util';
import templateUrl from './copy-box.html?url';

angular.module('biggraph').directive('copyBox', ["util", function(util) {
  return {
    restrict: 'E',
    scope: { data: '@', description: '@' },
    templateUrl,
    link: function(scope, element) {
      const client = new ClipboardJS(element.find('.clicky')[0]);
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
}]);
