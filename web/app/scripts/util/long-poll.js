// Long poll communication with the backend. A single long poll channel is used for all
// communications to avoid taking up more than one connection indefinitely.
// This service is mocked out in tests.
'use strict';
import '../app';
import './util';

angular.module('biggraph')
  .service('longPoll', function($timeout, util, $rootScope) {
    const that = this;

    // Contents of the last update.
    that.lastUpdate = {
      sparkStatus: { timestamp: 0, activeStages: [], pastStages: [] },
      progress: {},
    };

    // Registers a handler for updates.
    that.onUpdate = function(scope, handler) {
      scope.$on('long poll update', function(event, status) { handler(status); });
    };

    // Sets the list of state IDs we want updates on.
    that.setStateIds = function(stateIds) {
      that._stateIds = stateIds;
      that._interrupt();
    };

    that._stateIds = [];
    that._interrupt = function() {}; // Sends a new request immedately.
    function load() {
      const req = util.nocache('/ajax/long-poll', {
        syncedUntil: that.lastUpdate.sparkStatus.timestamp,
        stateIds: that._stateIds,
      });
      const interrupt = new Promise(function(resolve) {
        that._interrupt = function() {
          req.$abandon();
          resolve(that.lastUpdate);
        };
      });
      // Wait until either we get a response or _interrupt() is called.
      Promise.race([req, interrupt])
        .then(function(update) {
          update.received = Date.now();
          that.lastUpdate = update;
          $rootScope.$broadcast('long poll update', update);
          load();
        })
        .catch(function(error) {
          that.lastUpdate.error = error;
          $timeout(load, 10000); // Try again in a bit.
        });
    }
    load();
  });
