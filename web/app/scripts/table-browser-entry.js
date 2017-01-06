// Represents an entry in the SQL table browser.
// Parameters:
//  markedText: the name of the entry (i.e. table or column name)
//    some parts of the name can be highlighted in bold.
//    (See computeMatch() in table-browser.js for this datastructure.)
//  draggableText: This is the text that will be copied if the
//    user drags this entry into a text editor field.
'use strict';

angular.module('biggraph').directive('tableBrowserEntry', function() {
  return {
    restrict: 'E',
    scope: {
      markedText: '=',
      draggableText: '='
    },
    templateUrl: 'table-browser-entry.html',
    link: function(scope, element) {
      element.bind('dragstart', function(event) {
        console.log('DRAGSTAT ', event);
        event.originalEvent.dataTransfer.setData(
            'text',
            scope.draggableText);
      });
    }
  };
});
