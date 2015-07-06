module.exports = {
  addTo: function(browser) {
    browser.addMockModule('biggraph', function() {
      angular.module('biggraph').service('sparkStatusUpdater', function() {
        this.bind = function(scope, name) {
          scope[name] = { timestamp: 0, activeStages: [], pastStages: [] };
        };
      });
    });
  },
};
