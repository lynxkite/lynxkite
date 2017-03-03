// The toolbox shows the list of operation categories and the operations.
'use strict';

angular.module('biggraph').directive('operationSelectorEntry', function() {
  return {
    restrict: 'E',
    scope: {
      op: '=',
    },
    templateUrl: 'scripts/workspace/operation-selector-entry.html',
    link: function(scope, element) {
      element.bind('dragstart', function(event) {
        var data = JSON.stringify(scope.op);
        event.originalEvent.dataTransfer.setData(
            'text',
            data);
      });
    }
  };
});

