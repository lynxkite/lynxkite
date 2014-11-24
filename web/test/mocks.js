module.exports = {
  add: function(browser) {
    browser.addMockModule('biggraph', function() {
      angular.module('biggraph').service('sparkStatusUpdater', function() {
        this.bind = function() {};
      });       
    });
  },
};
