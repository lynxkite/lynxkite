// Long poll communication with the backend. A single long poll channel is used for all
// communications to avoid taking up more than one connection indefinitely.
// This service is mocked out in tests.
'use strict';

angular.module('biggraph')
.service('longPoll', function($timeout, util, $rootScope) {
  var that = this;
  that.lastUpdate = { timestamp: 0, activeStages: [], pastStages: [] };
  function load() {
    util.nocache('/ajax/spark-status', { syncedUntil: that.lastUpdate.timestamp })
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
