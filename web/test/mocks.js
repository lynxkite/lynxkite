'use strict';
module.exports = {
  addTo: function(browser) {
    browser.addMockModule('biggraph', function() {
      angular.module('biggraph')
        .service('longPoll', function() {
          this.onUpdate = function() {};
          this.setStateIds = function() {};
          this.lastUpdate = {
            sparkStatus: { timestamp: 0, activeStages: [], pastStages: [] },
            progress: {},
          };
        })
        .service('dropTooltipConfig', function() {
          this.enabled = false;
        })
        .service('environment', function() {
          this.protractor = true;
          this.vegaConfig = { renderer: 'svg' };
        });
      document.body.className += ' notransition'; // Disable CSS transitions.
    });
  },
};
