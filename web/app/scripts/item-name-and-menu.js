'use strict';

angular.module('biggraph').directive('itemNameAndMenu', function($timeout, util) {
  return {
    restrict: 'E',
    scope: { menu: '=', name: '@', type: '@' },
    templateUrl: 'item-name-and-menu.html',
    link: function(scope, element, attrs) {
      scope.util = util;
      scope.spaced = attrs.spaced !== undefined;

      scope.toggleRenaming = function() {
        scope.renaming = !scope.renaming;
        scope.newName = scope.name;
        // Focus #renameBox once it has appeared.
        $timeout(function() { element.find('#renameBox').focus(); });
      };

      scope.captureClick = function(event) {
        if (event) {
          event.originalEvent.alreadyHandled = true;
        }
      };

      scope.applyRenaming = function() {
        scope.renaming = false;
        scope.menu.rename(scope.type, scope.name, scope.newName);
      };

      scope.discard = function() {
        scope.menu.discard(scope.type, scope.name);
      };

      scope.duplicate = function() {
        scope.menu.duplicate(scope.type, scope.name);
      };
    },
  };
});
