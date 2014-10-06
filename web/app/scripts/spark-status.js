'use strict';

angular.module('biggraph').directive('sparkStatus', function($timeout, util) {
  return {
    restrict: 'E',
    scope: {},
    templateUrl: 'spark-status.html',
    link: function(scope) {
      scope.status = { timestamp: 0 };
      load();
      function load() {
        scope.update = util.nocache('/ajax/spark-status', { timestamp: scope.status.timestamp });
      }
      scope.$watch('update', update);
      scope.$watch('update.$resolved', update);
      function update() {
        if (scope.update && scope.update.$resolved) {
          // -1 is used for the timestamp in Grunt testing. We throttle in this case.
          if (scope.update.error && scope.update.timestamp === -1) {
            $timeout(load, 1000);  // Try again in a second.
          } else {
            scope.status = scope.update;
            load();
          }
        }
      }

      scope.kill = function() {
        util.post('/ajax/spark-cancel-jobs', { fake: 1 });
      };
    },
  };
});
