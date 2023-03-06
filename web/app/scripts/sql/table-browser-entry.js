// Represents an entry in the SQL table browser.
// Parameters:
//  text: the user-visible name of the entry (i.e. table or column name)
//  draggableText: This is the text that will be copied if the
//    user drags this entry into a text editor field.
'use strict';
import '../app';

angular.module('biggraph').directive('tableBrowserEntry', function() {
  return {
    restrict: 'E',
    scope: {
      text: '=',
      draggableText: '=',
    },
    templateUrl: 'scripts/sql/table-browser-entry.template',
    link: function(scope, element) {
      element.bind('dragstart', function(event) {
        event.originalEvent.dataTransfer.setData(
          'text',
          scope.draggableText);
      });
    }
  };
});
