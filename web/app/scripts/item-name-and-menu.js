'use strict';

angular.module('biggraph').directive('itemNameAndMenu', function($timeout) {
  return {
    restrict: 'E',
    scope: { side: '=', name: '@', type: '@' },
    templateUrl: 'item-name-and-menu.html',
    link: function(scope, element) {
      scope.toggleRenaming = function() {
        scope.renaming = !scope.renaming;
        scope.newName = scope.name;
        // Focus #renameBox once it has appeared.
        $timeout(function() { element.find('#renameBox').focus(); });
      };

      scope.applyRenaming = function() {
        scope.renaming = false;
        scope.side.rename(scope.type, scope.name, scope.newName);
      };

      scope.discard = function() {
        scope.side.discard(scope.type, scope.name);
      };

      scope.clone = function() {
        scope.side.clone(scope.type, scope.name);
      };
    },
  };
});
