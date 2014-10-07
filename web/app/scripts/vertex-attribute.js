'use strict';

angular.module('biggraph').directive('vertexAttribute', function($timeout) {
  return {
    scope: { attr: '=vertexAttribute', side: '=' },
    templateUrl: 'vertex-attribute.html',
    link: function(scope, element) {
      scope.toggleRenaming = function() {
        scope.renaming = !scope.renaming;
        scope.newName = scope.attr.title;
        // Focus #renameBox once it has appeared.
        $timeout(function() { element.find('#renameBox').focus(); });
      };

      scope.showLogCheckbox = function() {
        if (!scope.attr.isNumeric) { return false; }
        if (scope.histogram) { return true; }
        if (scope.side.state.graphMode === 'bucketed') {
          if (scope.side.state.xAttributeTitle === scope.attr.title) { return true; }
          if (scope.side.state.yAttributeTitle === scope.attr.title) { return true; }
        }
        return false;
      };
    },
  };
});
