'use strict';

angular.module('biggraph').directive('operationToolbox', function() {
  return {
    restrict: 'E',
    scope: { side: '=' },
    replace: true,
    templateUrl: 'operation-toolbox.html',
    link: function(scope) {
      function close() {
        scope.active = undefined;
      }
      function open(cat) {
        scope.active = cat;
      }
      scope.clicked = function(cat) {
        if (scope.active === cat) {
          close();
        } else {
          open(cat);
        }
      };
    },
  };
});
