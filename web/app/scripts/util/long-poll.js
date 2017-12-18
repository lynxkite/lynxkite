// Long poll communication with the backend. A single long poll channel is used for all
// communications to avoid taking up more than one connection indefinitely.
// This service is mocked out in tests.
'use strict';

angular.module('biggraph')
.service('longPoll', function($timeout, util, $rootScope) {
  var that = this;
  that.lastUpdate = {
    sparkStatus: { timestamp: 0, activeStages: [], pastStages: [] },
    progress: {},
  };
  that.onUpdate = function(scope, handler) {
    scope.$on('long poll update', function(event, status) { handler(status); });
  };
  that._stateIds = [];
  that._interrupt = function() {};
  that.setStateIds = function(stateIds) {
    that._stateIds = stateIds;
    that._interrupt();
  };
  function load() {
    var interrupt = new Promise(function(resolve) {
      that._interrupt = function() {
        resolve(that.lastUpdate);
      };
    });
    var req = util.nocache('/ajax/long-poll', {
      syncedUntil: that.lastUpdate.sparkStatus.timestamp,
      stateIds: that._stateIds,
    });
    Promise.race([req, interrupt])
      .then(function(update) {
        update.received = Date.now();
        that.lastUpdate = update;
        $rootScope.$broadcast('long poll update', update);
        load();
      })
      .catch(function(error) {
        that.lastUpdate.error = error;
        $timeout(load, 10000);  // Try again in a bit.
      });
  }
  load();
});
