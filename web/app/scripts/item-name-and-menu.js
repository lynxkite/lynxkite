// An item name with a drop-down menu providing "rename", "discard", and "duplicate" operations.
'use strict';

angular.module('biggraph').directive('itemNameAndMenu', function($timeout, util) {
  return {
    restrict: 'E',
    scope: { menu: '=', name: '@', type: '@', shortName: '@', config: '=?' },
    templateUrl: 'item-name-and-menu.html',
    link: function(scope, element) {
      scope.util = util;
      scope.shortName = scope.shortName || scope.name;

      scope.toggleRenaming = function() {
        scope.renaming = !scope.renaming;
        if (scope.renaming) {
          scope.newName = scope.name;
          // Focus #renameBox once it has appeared.
          $timeout(function() { element.find('#renameBox').focus(); });
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

      scope.editConfig = function() {
        scope.menu.editConfig(scope.name, scope.config, scope.type);
      };
    },
  };
});
