'use strict';

// A context menu for graph vertices.
angular.module('biggraph').directive('vertexMenu', function() {
  return {
    replace: true,
    restrict: 'E',
    scope: { model: '=' },
    templateUrl: 'vertex-menu.html',
    link: function() {
    },
  };
});
