'use strict';

angular.module('biggraph').directive('edgeAttribute', function($timeout) {
  return {
    scope: { attr: '=edgeAttribute', side: '=' },
    templateUrl: 'edge-attribute.html',
    link: function(scope, element) {
      scope.toggleRenaming = function() {
        scope.renaming = !scope.renaming;
        scope.newName = scope.attr.title;
        // Focus #renameBox once it has appeared.
        $timeout(function() { element.find('#renameBox').focus(); });
      };
    },
  };
});
