'use strict';

/* global element, by, protractor */

var request = require('request');

module.exports = (function() {
  var K = protractor.Key;  // Short alias.
  return {
    evaluateOnLeftSide: function(expr) {
      return element(by.css('#side-left')).evaluate(expr);
    },

    expectCurrentProjectIs: function(name) {
      expect(browser.getCurrentUrl()).toContain('/#/project/' + name);
    },

    expectHelpPopupVisible: function(helpId, isVisible) {
      expect(element(by.css('div[help-id="' + helpId + '"]')).isDisplayed()).toBe(isVisible);
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

    openLeftOperation: function(name) {
      element(by.css('#operation-toolbox-left #operation-search')).click();
      element(by.css('#operation-toolbox-left #filter')).sendKeys(name, K.ENTER);
    },

    openNewProject: function(name) {
      element(by.id('new-project')).click();
      element(by.id('new-project-name')).sendKeys(name, K.ENTER);
    },

    runLeftOperation: function(name, params) {
      params = params || {};
      this.openLeftOperation(name);
      for (var key in params) {
        var p = '#operation-toolbox-left operation-parameters #' + key + ' input';
        element(by.css(p)).sendKeys(params[key], K.ENTER);
      }

      element(by.css('#operation-toolbox-left .ok-button')).click();
    },

    setLeftAttributeFilter: function(attributeName, filterValue) {
      var filterBox = element(
        by.css('#side-left .attribute input[name="' + attributeName + '"]'));
      filterBox.sendKeys(filterValue, K.ENTER);
    },

    toggleLeftSampledVisualization: function() {
      element(by.css('#side-left label[btn-radio="\'sampled\'"]')).click();
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
