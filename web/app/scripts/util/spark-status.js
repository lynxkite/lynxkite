// The Spark status indicator cogwheel in the bottom left.
'use strict';

angular.module('biggraph')
.directive('sparkStatus', function(util, sparkStatusUpdater) {
  return {
    restrict: 'E',
    scope: {},
    templateUrl: 'scripts/util/spark-status.html',
    link: function(scope) {
      sparkStatusUpdater.bind(scope, 'status');

      scope.kill = function() {
        util.post('/ajax/spark-cancel-jobs', { fake: 1 });
      };

      scope.stages = function(status) {
        for (var i = 0; i < status.activeStages.length; ++i) {
          status.activeStages[i].active = true;
        }
        return status.activeStages.concat(status.pastStages);
      };

      var hashColors = {}; // Cache for this surprisingly costly method.
      scope.hashToColor = function(active, hash) {
        if (!(hash in hashColors)) {
          hash = Math.abs(hash);
          /* global tinycolor */
          hashColors[hash] = {
            true: tinycolor({ h: hash % 360, s: 1.0, l: 0.5 }).toString(),
            false: tinycolor({ h: hash % 360, s: 1.0, l: 0.5 }).toString() };
        }
        return hashColors[hash][active];
      };

      scope.height = function(stage) {
        return (20 * stage.tasksCompleted / stage.size).toFixed(1) + 'px';
      };

      scope.message = function(status) {
        var last = 0;
        for (var i = 0; i < status.activeStages.length; ++i) {
          var stage = status.activeStages[i];
          // Correct against client/server clock offset using status.received.
          var t = Date.now() - stage.lastTaskTime + status.timestamp - status.received;
          if (t > last) {
            last = t;
          }
        }
        return 'Last progress ' + (last / 1000).toFixed() + ' seconds ago.';
      };

      scope.normalOperation = function(status) {
        return !status.error &&
          status.activeStages.length > 0 &&
          status.sparkWorking &&
          status.kiteCoreWorking;
      };

      scope.stagesTooltip = function() {
        var status = scope.status;
        var tooltip = 'Spark stages';
        if (status.activeExecutorNum !== undefined && status.configedExecutorNum !== undefined) {
          tooltip = tooltip + ' (' + status.activeExecutorNum + ' out of ' +
            status.configedExecutorNum + ' executors running)';
        }
        return tooltip + '.';
      };
    },
  };
})
// The status is updated in a "long poll". The server delays the response until
// there is an update. It is implemented in a service so that tests can mock it out.
.service('sparkStatusUpdater', function($timeout, util) {
  this.bind = function(scope, name) {
    scope[name] = { timestamp: 0, activeStages: [], pastStages: [] };
    var update;
    function load() {
      update = util.nocache('/ajax/spark-status', { syncedUntil: scope[name].timestamp });
    }
    function onUpdate() {
      if (update && update.$resolved) {
        if (update.$error) {
          scope[name].error = update.$error;
          $timeout(load, 10000);  // Try again in a bit.
        } else {
          update.received = Date.now();
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
