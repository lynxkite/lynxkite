'use strict';

/* global element, by, protractor */

var request = require('request');
var K = protractor.Key;  // Short alias.

function Side(direction) {
  this.direction = direction;
  this.side = element(by.id('side-' + direction));
  this.toolbox = element(by.id('operation-toolbox-' + direction));
}

Side.prototype = {
  evaluate: function(expr) {
    return this.side.evaluate(expr);
  },

  applyFilters: function() {
    return this.side.element(by.id('apply-filters-button')).click();
  },

  getValue: function(id) {
    var asStr = this.side.element(by.css('value#' + id + ' span.value')).getText();
    return asStr.then(function(asS) { return parseInt(asS); });
  },

  edgeCount: function() {
    return this.getValue('edge-count');
  },

  vertexCount: function() {
    return this.getValue('vertex-count');
  },

  segmentCount: function() {
    return this.getValue('segment-count');
  },

  openOperation: function(name) {
    this.toolbox.element(by.id('operation-search')).click();
    this.toolbox.element(by.id('filter')).sendKeys(name, K.ENTER);
  },

  openSegmentation: function(segmentationName) {
    this.side.element(by.id('segmentation-' + segmentationName)).click();
  },

  runOperation: function(name, params) {
    params = params || {};
    this.openOperation(name);
    for (var key in params) {
      var p = 'operation-parameters #' + key + ' .operation-attribute-entry';
      this.sendKeysToElement(element(by.css(p)), params[key]);
    }

    this.toolbox.element(by.css('.ok-button')).click();
  },

  setAttributeFilter: function(attributeName, filterValue) {
    var filterBox = this.side.element(
      by.css('.attribute input[name="' + attributeName + '"]'));
    filterBox.sendKeys(filterValue, K.ENTER);
  },

  toggleSampledVisualization: function() {
    this.side.element(by.css('label[btn-radio="\'sampled\'"]')).click();
  },
};

module.exports = (function() {
  return {
    left: new Side('left'),
    right: new Side('right'),

    expectCurrentProjectIs: function(name) {
      expect(browser.getCurrentUrl()).toContain('/#/project/' + name);
    },

    expectHelpPopupVisible: function(helpId, isVisible) {
      expect(element(by.css('div[help-id="' + helpId + '"]')).isDisplayed()).toBe(isVisible);
    },

    openNewProject: function(name) {
      element(by.id('new-project')).click();
      element(by.id('new-project-name')).sendKeys(name, K.ENTER);
    },

    openSegmentation: function(segmentationName) {
      var s = '#side-left .segmentation #segmentation-' + segmentationName;
      element(by.css(s)).click();
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
