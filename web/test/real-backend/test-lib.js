'use strict';

/* global element, by, protractor */

var request = require('request');

module.exports = (function() {
  var K = protractor.Key;  // Short alias.
  return {
    openNewProject: function(name) {
      element(by.id('new-project')).click();
      element(by.id('new-project-name')).sendKeys(name, K.ENTER);
    },

    expectCurrentProjectIs: function(name) {
      expect(browser.getCurrentUrl()).toContain('/#/project/' + name);
    },

    runLeftOperation: function(name) {
      element(by.css('#operation-toolbox-left #operation-search')).click();
      element(by.css('#operation-toolbox-left #filter')).sendKeys(name, K.ENTER);
      element(by.css('#operation-toolbox-left .ok-button')).click();
    },

    leftVertexCount: function() {
      var asStr = element(by.css('#side-left value.vertex-count span.value')).getText();
      return asStr.then(function(asS) { return parseInt(asS); });
    },

    // Deletes all projects and directories.
    discardAll: function() {
      var defer = protractor.promise.defer();
      request.post(
        browser.baseUrl + 'ajax/discardAllReallyIMeanIt',
        { json: { fake: 1 } },
        function(error, message) {
          if (error || message.statusCode >= 400) {
            defer.reject({ error : error, message : message });
          } else {
            defer.fulfill();
          }
        });
      browser.controlFlow().execute(function() { return defer.promise; });
    },
  };
})();
