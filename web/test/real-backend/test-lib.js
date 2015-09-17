'use strict';

/* global element */
/* global by */
/* global protractor */

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
  };
})();
