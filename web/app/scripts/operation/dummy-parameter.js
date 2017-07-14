// Displays dummy parameter.
'use strict';

angular.module('biggraph').directive('dummyParameter',
  function() {
    return {
      restrict: 'E',
      scope: {
        param: '='
      },
      templateUrl: 'scripts/operation/dummy-parameter.html'
    };
  }
);
