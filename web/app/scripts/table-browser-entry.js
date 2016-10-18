// Presents the parameters for running SQL scripts.
'use strict';

angular.module('biggraph').directive('tableBrowserEntry', function() {
  return {
    restrict: 'E',
    scope: {
      markedText: '=',
      plainTextFallback: '=',
      draggableText: '='
    },
    templateUrl: 'table-browser-entry.html',
    link: function(scope, element) {
      element.bind('dragstart', function(event) {
        event.originalEvent.dataTransfer.setData(
            'text',
            scope.draggableText);
      });
    }
  };
});
