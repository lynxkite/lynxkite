// Displays dummy parameter.
import '../app';
import templateUrl from './dummy-parameter.html?url';

angular.module('biggraph').directive('dummyParameter',
  function() {
    return {
      restrict: 'E',
      scope: {
        param: '=',
        htmlId: '='
      },
      templateUrl,
    };
  }
);
