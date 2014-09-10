'use strict';

angular.module('biggraph').directive('operationToolbox', function() {
  return {
    restrict: 'E',
    scope: { side: '=' },
    replace: true,
    templateUrl: 'operation-toolbox.html',
    link: function(scope) {
      scope.clickedCat = function(cat) {
        if (scope.active === cat && !scope.op) {
          scope.active = undefined;
        } else {
          scope.active = cat;
        }
        scope.op = undefined;
      };
      scope.clickedOp = function(op) {
        scope.op = op;
      };
    },
  };
});
