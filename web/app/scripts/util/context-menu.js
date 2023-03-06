// A context menu for graph vertices.
'use strict';
import '../app';

angular.module('biggraph').directive('contextMenu', function($timeout) {
  return {
    replace: true,
    restrict: 'E',
    scope: { model: '=' },
    templateUrl: 'scripts/util/context-menu.html',
    link: function(scope, element) {
      scope.isEmpty = function(o) { return !o || angular.equals(o, {}); };
      scope.$watch('model.enabled', function(enabled) {
        if (enabled) {
          // Whenever the menu opens we set focus on it. We need this so we can close it on blur
          // (that is, if the user mouse downs anywhere outside).
          $timeout(function() { element.focus(); });
        }
      });
      scope.executeAction = function(action) {
        action.callback(scope.model.data);
        scope.model.enabled = false;
      };
    },
  };
});
