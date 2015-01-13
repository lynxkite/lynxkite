'use strict';

angular.module('biggraph').directive('value', function(util) {
  return {
    restrict: 'E',
    scope: { ref: '=', details: '=' },
    templateUrl: 'value.html',
    link: function(scope) {
      scope.reportError = function() {
        console.log('ref', scope.ref, 'details', scope.details);
        if (!scope.ref) {
          util.reportError({
            message: 'undefined reference',
            details: scope.details,
          });
        } else {
          var details = {
            url: scope.ref.$url,
            params: scope.ref.$params,
            details: scope.ref.details,
          };
          util.reportError({
            message: scope.ref.$error,
            details: details,
          });
        }
      };
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
