'use strict';

angular.module('biggraph').directive('scalar', function($timeout) {
  return {
    scope: { scalar: '=', value: '@', side: '=' },
    templateUrl: 'scalar.html',
    link: function(scope, element) {
      scope.toggleRenaming = function() {
        scope.renaming = !scope.renaming;
        scope.newName = scope.scalar.title;
        // Focus #renameBox once it has appeared.
        $timeout(function() { element.find('#renameBox').focus(); });
      };
    },
  };
});
