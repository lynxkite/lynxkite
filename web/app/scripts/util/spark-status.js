// The Spark status indicator cogwheel in the bottom left.
'use strict';
import '../app';
import './util';
import chroma from 'chroma-js';
import templateUrl from './spark-status.html?url';

angular.module('biggraph')
  .directive('sparkStatus', ["util", "longPoll", function(util, longPoll) {
    return {
      restrict: 'E',
      scope: {},
      templateUrl,
      link: function(scope) {
        scope.status = longPoll.lastUpdate.sparkStatus;
        longPoll.onUpdate(scope, function(status) { scope.status = status.sparkStatus; });

        scope.kill = function() {
          util.post('/ajax/spark-cancel-jobs', { fake: 1 });
        };

        scope.stages = function(status) {
          for (let i = 0; i < status.activeStages.length; ++i) {
            status.activeStages[i].active = true;
          }
          return status.activeStages.concat(status.pastStages);
        };

        const hashColors = {}; // Cache for this surprisingly costly method.
        scope.hashToColor = function(active, hash) {
          hash = Math.abs(hash);
          if (!(hash in hashColors)) {
          /* global chroma */
            hashColors[hash] = {
              true: chroma({ h: hash % 360, s: 1.0, l: 0.5 }).toString(),
              false: chroma({ h: hash % 360, s: 1.0, l: 0.9 }).toString() };
          }
          return hashColors[hash][active];
        };

        scope.height = function(stage) {
          return (20 * stage.tasksCompleted / stage.size).toFixed(1) + 'px';
        };

        scope.message = function(status) {
          let last = 0;
          for (let i = 0; i < status.activeStages.length; ++i) {
            const stage = status.activeStages[i];
            // Correct against client/server clock offset using status.received.
            const t = Date.now() - stage.lastTaskTime + status.timestamp - status.received;
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
          const status = scope.status;
          let tooltip = 'Spark stages';
          if (status.activeExecutorNum !== undefined && status.configedExecutorNum !== undefined) {
            tooltip = tooltip + ' (' + status.activeExecutorNum + ' out of ' +
            status.configedExecutorNum + ' executors running)';
          }
          return tooltip + '.';
        };
      },
    };
  }]);
