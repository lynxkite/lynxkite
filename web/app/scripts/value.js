'use strict';

angular.module('biggraph').directive('value', function(util) {
  return {
    restrict: 'E',
    scope: { ref: '=' },
    templateUrl: 'value.html',
    link: function(scope) {
      scope.human = true;
      scope.humanized = function(ref) {
        return scope.human && ref.double && ref.double.toString() !== util.human(ref.double);
      };
      scope.format = function(ref) {
        return util.human(ref.double);
      };
      scope.setHuman = function(h) {
        scope.human = h;
      };
    },
  };
});
