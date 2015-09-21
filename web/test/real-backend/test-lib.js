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

    openLeftProjectHistory: function() {
      element(by.css('#side-left .history-button')).click();
    },

    openLeftOperation: function(name) {
      element(by.css('#operation-toolbox-left #operation-search')).click();
      element(by.css('#operation-toolbox-left #filter')).sendKeys(name, K.ENTER);
    },

    openNewProject: function(name) {
      element(by.id('new-project')).click();
      element(by.id('new-project-name')).sendKeys(name, K.ENTER);
    },

    openSegmentation: function(segmentationName) {
      var s = '#side-left .segmentation #segmentation-' + segmentationName;
      element(by.css(s)).click();
    },

    runLeftOperation: function(name, params) {
      params = params || {};
      this.openLeftOperation(name);
      for (var key in params) {
        var p = '#operation-toolbox-left operation-parameters #' + key + ' .operation-attribute-entry';
        this.sendKeysToElement(element(by.css(p)), params[key]);
      }

      element(by.css('#operation-toolbox-left .ok-button')).click();
    },

    segmentCount: function() {
      var asStr = element(by.css('#side-right value#segment-count span.value')).getText();
      return asStr.then(function(asS) { return parseInt(asS); });
    },

    sendKeysToElement: function(e, keys) {
      // ACE editor and non-ace controls need different handling.
      e.getAttribute('data-kind').then(
          function(dataKind) {
            expect(['text', 'select', 'text-ace']).toContain(dataKind);
            if (dataKind === 'text' || dataKind === 'select') {
              // Normal input control.
              e.sendKeys(keys);
            } else if (dataKind === 'text-ace') {
              // ACE editor control.
              var aceContent = e.element(by.css('div.ace_content'));
              var aceInput = e.element(by.css('textarea.ace_text-input'));
              browser.actions().click(aceContent).perform();
              browser.actions().doubleClick(aceContent).perform();
              aceInput.sendKeys(keys);
            }
          });
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
