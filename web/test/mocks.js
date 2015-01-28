module.exports = {
  addTo: function(browser) {
    browser.addMockModule('biggraph', function() {
      angular.module('biggraph').service('sparkStatusUpdater', function() {
        this.bind = function() {};
      });       
    });
  },
};
