// Displays dummy parameter.
'use strict';
import '../app';

angular.module('biggraph').directive('dummyParameter',
  function() {
    return {
      restrict: 'E',
      scope: {
        param: '=',
        htmlId: '='
      },
      templateUrl: 'scripts/operation/dummy-parameter.template'
    };
  }
);
