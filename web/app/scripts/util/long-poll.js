// Long poll communication with the backend. A single long poll channel is used for all
// communications to avoid taking up more than one connection indefinitely.
// This service is mocked out in tests.
import '../app';
import './util';

angular.module('biggraph')
  .service('longPoll', ['$timeout', 'util', '$rootScope', function($timeout, util, $rootScope) {
    // Contents of the last update.
    this.lastUpdate = {
      sparkStatus: { timestamp: 0, activeStages: [], pastStages: [] },
      progress: {},
    };

    // Registers a handler for updates.
    this.onUpdate = (scope, handler) => {
      scope.$on('long poll update', function(event, status) { handler(status); });
    };

    // Sets the list of state IDs we want updates on.
    this.setStateIds = (stateIds) => {
      this._stateIds = stateIds;
      this._interrupt();
    };

    this._stateIds = [];
    this._interrupt = function() {
      // Sends a new request immedately.
    };
    const load = () => {
      const req = util.nocache('/ajax/long-poll', {
        syncedUntil: this.lastUpdate.sparkStatus.timestamp,
        stateIds: this._stateIds,
      });
      const interrupt = new Promise((resolve) => {
        this._interrupt = () => {
          req.$abandon();
          resolve(this.lastUpdate);
        };
      });
      // Wait until either we get a response or _interrupt() is called.
      Promise.race([req, interrupt])
        .then((update) => {
          update.received = Date.now();
          this.lastUpdate = update;
          $rootScope.$broadcast('long poll update', update);
          load();
        })
        .catch((error) => {
          this.lastUpdate.error = error;
          $timeout(load, 10000); // Try again in a bit.
        });
    };
    load();
  }]);
