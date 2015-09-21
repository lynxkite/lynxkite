'use strict';

/* global element, by, protractor */

var request = require('request');
var K = protractor.Key;  // Short alias.
var testLib;  // Forward declaration.

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

  openProjectHistory: function() {
    this.side.element(by.css('.history-button')).click();
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
      testLib.sendKeysToElement(element(by.css(p)), params[key]);
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

testLib = {
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

  sendKeysToElement: function(e, keys) {
    // ACE editor and non-ace controls need different handling.
    e.evaluate('param.kind').then(
        function(dataKind) {
          if (dataKind !== 'code') {
            // Normal input control.
            e.sendKeys(keys);
          } else {
            // ACE editor control.
            var aceContent = e.element(by.css('div.ace_content'));
            var aceInput = e.element(by.css('textarea.ace_text-input'));
            // The triple click on the text area focuses it and selects all its
            // text content. Therefore the first key sent will clear its current
            // content. (Caveat: if 'keys' is the empty string then it won't be
            // cleared.)
            browser.actions().click(aceContent).perform();
            browser.actions().doubleClick(aceContent).perform();
            aceInput.sendKeys(keys);
          }
        });
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

module.exports = testLib;
