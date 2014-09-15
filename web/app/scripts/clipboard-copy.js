'use strict';

// If the user copies an element with the clipboard-copy="foo" attribute to the clipboard,
// the text "foo" will be copied instead of the natural contents.
angular.module('biggraph').directive('clipboardCopy', function() {
  return {
    link: function(scope, element, attrs) {
      element.on('copy', function(e) {
        e.preventDefault();
        e.originalEvent.clipboardData.setData('text/plain', attrs.clipboardCopy);
      });
    },
  };
});
