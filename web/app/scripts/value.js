'use strict';

angular.module('biggraph').directive('value', function(util) {
  return {
    restrict: 'E',
    scope: { ref: '=' },
    templateUrl: 'value.html',
    link: function(scope, element, attrs) {
      scope.human = attrs.human !== undefined;
      scope.humanized = function(x) {
        return scope.human && x !== util.human(x);
      };
      scope.format = function(x) {
        return util.human(x);
      };
    },
  };
});
