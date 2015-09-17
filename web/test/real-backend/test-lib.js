'use strict';

module.exports = (function() {
  var mocks = require('../mocks.js');
  var K = protractor.Key;  // Short alias.
  return {
    initTest: function() {
      mocks.addTo(browser);
    },
    startPage: function() {
      browser.get('/');
    },
    openNewProject: function(name) {
      element(by.id('new-project')).click();
      element(by.id('new-project-name')).sendKeys(name, K.ENTER);
    },
    expectCurrentProjectIs: function(name) {
      expect(browser.getCurrentUrl()).toContain('/#/project/' + name);
    },
    runOperation: function(name) {
      element(by.id('operation-search')).click();
      element(by.css('operation-toolbox #filter')).sendKeys(name, K.ENTER);
      element(by.css('operation-toolbox .ok-button')).click();
    },
    currentVertexCount: function() {
      var asStr = element(by.css('value.vertex-count span.value')).getText();
      return asStr.then(function(asS) { return parseInt(asS); });
    },
  };
})();
