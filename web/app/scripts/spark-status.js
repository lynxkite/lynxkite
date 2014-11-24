'use strict';

angular.module('biggraph')
.directive('sparkStatus', function(util, sparkStatusUpdater) {
  return {
    restrict: 'E',
    scope: {},
    templateUrl: 'spark-status.html',
    link: function(scope) {
      sparkStatusUpdater.bind(scope, 'status');

      scope.kill = function() {
        util.post('/ajax/spark-cancel-jobs', { fake: 1 });
      };
    },
  };
})
// The status is updated in a "long poll". The server delays the response until
// there is an update. It is implemented in a service so that tests can mock it out.
.service('sparkStatusUpdater', function($timeout, util) {
  this.bind = function(scope, name) {
    scope[name] = { timestamp: 0 };
    var update;
    function load() {
      update = util.nocache('/ajax/spark-status', { syncedUntil: scope[name].timestamp });
    }
    function onUpdate() {
      if (update && update.$resolved) {
        if (update.$error) {
          $timeout(load, 10000);  // Try again in a bit.
        } else {
          scope[name] = update;
          load();
        }
      }
    }
    load();
    scope.$watch(function() { return update; }, onUpdate);
    scope.$watch(function() { return update.$resolved; }, onUpdate);
  };
});
