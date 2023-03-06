// An item name with a drop-down menu providing "rename", "discard", and "duplicate" operations.
'use strict';
import ClipboardJS from 'clipboard';
import '../app';
import '../util/util';
import templateUrl from './item-name-and-menu.html?url';

angular.module('biggraph').directive('itemNameAndMenu', ["$timeout", "util", "$location", function($timeout, util, $location) {
  return {
    restrict: 'E',
    scope: { object: '=', reload: '&' },
    templateUrl,
    link: function(scope, element) {
      scope.util = util;

      scope.toggleRenaming = function() {
        scope.renaming = !scope.renaming;
        if (scope.renaming) {
          scope.newName = scope.object.name;
          // Focus #renameBox once it has appeared.
          $timeout(function() { element.find('#renameBox').focus(); });
        }
      };

      scope.applyRenaming = function() {
        scope.renaming = false;
        if (scope.object.name === scope.newName) { return; }
        util.post('/ajax/renameEntry',
          { from: scope.object.name, to: scope.newName, overwrite: false }).then(scope.reload);
      };

      scope.discard = function() {
        let trashDir = 'Trash';
        if (scope.object.name.indexOf(trashDir) === 0) {
          // Already in Trash. Discard permanently.
          util.post('/ajax/discardEntry', { name: scope.object.name }).then(scope.reload);
        } else {
          // Not in Trash. Move to Trash.
          util.post('/ajax/renameEntry',
            { from: scope.object.name, to: trashDir + '/' + scope.object.name, overwrite: true })
            .then(scope.reload);
        }
      };

      scope.duplicate = function() {
        util.post('/ajax/forkEntry', {
          from: scope.object.name,
          to: util.dirName(scope.object.name) + 'Copy of ' + util.baseName(scope.object.name)
        }).then(scope.reload);
      };

      scope.openWorkspace = function() {
        $location.url('/workspace/' + scope.object.name);
      };

      /* global ClipboardJS */
      const clippy = new ClipboardJS('#menu-copy-to-clipboard');
      scope.$on('$destroy', () => clippy.destroy());
    },
  };
}]);
