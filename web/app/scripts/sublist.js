'use strict';

angular.module('biggraph').directive('sublist', function () {
  return {
    restrict: 'E',
    transclude: true,
    scope: { heading: '@' },
    replace: false,
    templateUrl: 'sublist.html',
    link: function(scope, element, attrs, ctrl, transclude) {
      element.find('#contents').replaceWith(transclude());
      scope.count = function() {
        return element.find('#collapse').children('.list-group-item').length;
      };
    },
  };
});
