'use strict';

angular.module('biggraph').directive('tokenfield', function() {
  return {
    restrict: 'A',
    priority: 100,  // This is like ng-list.
    require: 'ngModel',
    link: function(scope, element, attr, ctrl) {
      element.tokenfield();
      element.on('$destroy', function() { element.tokenfield('destroy'); });
      ctrl.$render = function() {
        element.tokenfield('setTokens', ctrl.$viewValue);
      };
      ctrl.$parsers.push(function(str) {
        return str.split(',').map(function(s) { return s.trim(); });
      });
    },
  };
});
