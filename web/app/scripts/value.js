'use strict';

angular.module('biggraph').directive('value', function(util) {
  return {
    restrict: 'E',
    scope: { ref: '=' },
    templateUrl: 'value.html',
    link: function(scope, element, attrs) {
      scope.human = attrs.human !== undefined;
      scope.format = function(x) {
        if (scope.human) {
          return util.human(x);
        } else {
          return x;
        }
      };
    },
  };
});
