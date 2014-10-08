'use strict';

angular.module('biggraph').directive('sparkStatus', function($timeout, util) {
  return {
    restrict: 'E',
    scope: {},
    templateUrl: 'spark-status.html',
    link: function(scope) {
      // The status is updated in a "long poll". The server delays the response until
      // there is an update.
      scope.status = { timestamp: 0 };
      load();
      function load() {
        scope.update = util.nocache('/ajax/spark-status', { syncedUntil: scope.status.timestamp });
      }
      scope.$watch('update', update);
      scope.$watch('update.$resolved', update);
      function update() {
        if (scope.update && scope.update.$resolved) {
          if (scope.update.error) {
            $timeout(load, 10000);  // Try again in a bit.
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
