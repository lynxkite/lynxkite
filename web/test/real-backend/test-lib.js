'use strict';

/* global element */
/* global by */
/* global protractor */

module.exports = (function() {
  var K = protractor.Key;  // Short alias.
  return {
    evaluateOnLeftSide: function(expr) {
      return element(by.css('#side-left')).evaluate(expr);
    },

    expectCurrentProjectIs: function(name) {
      expect(browser.getCurrentUrl()).toContain('/#/project/' + name);
    },

    getLeftHistogram: function(attributeName) {
      return element(by.css('#side-left histogram[attr-name="' + attributeName + '"]'));
    },

    getLeftHistogramButton: function(attributeName) {
      return element(by.css('#side-left #histogram-button[attr-name="' + attributeName + '"]'));
    },
    
    getLeftHistogramValues: function(attributeName) {
      var button = this.getLeftHistogramButton(attributeName);
      button.click();
      var histo = this.getLeftHistogram(attributeName);
      var tds = histo.all(by.css('td'));
      console.log('fuck');
      tds.then(function(alma) {
        for (var i = 0; i < alma.length; i++) {
          var egytd = alma[i];
          console.log('egytdstart');
          console.log(egytd);
          console.log('egytdend');
        }
        console.log('fuck2');
      });
      console.log('fuck3');
      button.click().then(function() { console.log('fuckx'); });
    },

    leftApplyFilters: function() {
      return element(by.css('#side-left #apply-filters-button')).click();
    },

    leftEdgeCount: function() {
      var asStr = element(by.css('#side-left value#edge-count span.value')).getText();
      return asStr.then(function(asS) { return parseInt(asS); });
    },

    leftVertexCount: function() {
      var asStr = element(by.css('#side-left value#vertex-count span.value')).getText();
      return asStr.then(function(asS) { return parseInt(asS); });
    },

    openNewProject: function(name) {
      element(by.id('new-project')).click();
      element(by.id('new-project-name')).sendKeys(name, K.ENTER);
    },

    runLeftOperation: function(name) {
      element(by.css('#operation-toolbox-left #operation-search')).click();
      element(by.css('#operation-toolbox-left #filter')).sendKeys(name, K.ENTER);
      element(by.css('#operation-toolbox-left .ok-button')).click();
    },

    setLeftAttributeFilter: function(attributeName, filterValue) {
      var filterBox = element(
        by.css('#side-left .attribute input[name="' + attributeName + '"]'));
      filterBox.sendKeys(filterValue, K.ENTER);    
    },
  };
})();
