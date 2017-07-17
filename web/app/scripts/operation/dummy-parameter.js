// Displays dummy parameter.
'use strict';

angular.module('biggraph').directive('dummyParameter',
  function() {
    return {
      restrict: 'E',
      scope: {
        param: '=',
        htmlId: '='
      },
      templateUrl: 'scripts/operation/dummy-parameter.html'
    };
  }
);
