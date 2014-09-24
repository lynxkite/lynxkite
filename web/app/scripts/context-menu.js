'use strict';

// A context menu for graph vertices.
angular.module('biggraph').directive('contextMenu', function() {
  return {
    replace: true,
    restrict: 'E',
    scope: { model: '=' },
    templateUrl: 'context-menu.html',
    link: function() {
    },
  };
});
