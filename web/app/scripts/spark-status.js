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
          scope.status = scope.update;
          // -1 is used for the timestamp in Grunt testing.
          // We throttle in this case to avoid a busy loop.
          if (scope.update.error || scope.update.timestamp === -1) {
            $timeout(load, 10000);  // Try again in a bit.
          } else {
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
