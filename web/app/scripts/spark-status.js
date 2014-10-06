'use strict';

angular.module('biggraph').directive('sparkStatus', function(util) {
  return {
    restrict: 'E',
    scope: {},
    templateUrl: 'spark-status.html',
    link: function(scope) {
      scope.status = util.nocache('/ajax/spark-status', { timestamp: 0 });
      scope.$watch('status', repeat);
      scope.$watch('status.$resolved', repeat);
      function repeat() {
        if (scope.status && scope.status.$resolved) {
          scope.update = util.nocache('/ajax/spark-status', { timestamp: scope.status.timestamp });
        }
      }
      scope.$watch('update', update);
      scope.$watch('update.$resolved', update);
      function update() {
        if (scope.update && scope.update.$resolved) {
          scope.status = scope.update;
        }
      }
    },
  };
});
