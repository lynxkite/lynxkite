'use strict';

angular.module('biggraph').directive('vertexAttribute', function() {
  return {
    scope: { attr: '=vertexAttribute', side: '=' },
    templateUrl: 'vertex-attribute.html',
    link: function(scope) {
      scope.showLogCheckbox = function() {
        if (!scope.attr.isNumeric) { return false; }
        if (scope.histogram) { return true; }
        if (scope.side.graphing() === 'bucketed') {
          if (scope.side.state.xAttributeTitle === scope.attr.title) { return true; }
          if (scope.side.state.yAttributeTitle === scope.attr.title) { return true; }
        }
        return false;
      };
    },
  };
});
