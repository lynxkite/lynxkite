'use strict';

angular.module('biggraph').directive('sideOperationToolbox', function($rootScope, hotkeys) {
  return {
    restrict: 'E',
    scope: { side: '=' },
    templateUrl: 'side-operation-toolbox.html',
    link: function(scope) {
      scope.box = {};
      if (scope.side.primary) {  // Set up hotkeys on the primary project only.
        var hk = hotkeys.bindTo(scope);
        hk.add({ combo: '/', description: 'Find operation', callback: function(e) {
          e.preventDefault();  // Do not type "/".
          scope.$broadcast('open operation search');
        }});
        hk.add({ combo: 'esc', allowIn: ['INPUT'], callback: function() {
          if (scope.box.op) {
            scope.box.op = undefined;
          } else if (scope.box.searching) {
            scope.box.searching = undefined;
          } else if (scope.box.category) {
            scope.box.category = undefined;
          }
        }});
      }

      scope.$watch('box.category || box.searching', function(open) {
        if (open) {
          $rootScope.$broadcast('close all the other side-operation-toolboxes', scope);
        }
      });
      scope.$on('close all the other side-operation-toolboxes', function(e, source) {
        if (scope !== source) {
          scope.box.op = undefined;
          scope.box.category = undefined;
          scope.box.searching = undefined;
        }
      });

      scope.$on('apply operation', function() {
        scope.box.applying = true;
        scope.side.applyOp(scope.box.op, scope.box.params)
          .then(function() { scope.box.applying = false; });
      });
    },
  };
});
