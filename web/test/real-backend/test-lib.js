'use strict';

/* global element, by, protractor */

var testLib; // Forward declaration.
var request = require('request');
var K = protractor.Key;  // Short alias.

function Side(direction) {
  this.direction = direction;
  this.side = element(by.id('side-' + direction));
  this.toolbox = element(by.id('operation-toolbox-' + direction));
}

Side.prototype = {
  close: function() {
    this.side.element(by.id('close-project')).click();
  },

  evaluate: function(expr) {
    return this.side.evaluate(expr);
  },

  applyFilters: function() {
    return this.side.element(by.id('apply-filters-button')).click();
  },

  getCategorySelector: function(categoryTitle) {
    return this.toolbox.element(by.css('div.category[tooltip="' + categoryTitle + '"]'));
  },

  getCloseHistoryButton: function() {
    return this.side.element(by.id('close-history-button'));
  },

  getHistogram: function(attributeName) {
    return this.side.element(by.css('histogram[attr-name="' + attributeName + '"]'));
  },

  getHistogramButton: function(attributeName) {
    return this.side.element(by.css('#histogram-button[attr-name="' + attributeName + '"]'));
  },

  getHistogramTotalElement: function(attributeName) {
    return this.getHistogram(attributeName).element(by.css('.histogram-total'));
  },

  getHistogramValues: function(attributeName) {
    var button = this.getHistogramButton(attributeName);
    var total = this.getHistogramTotalElement(attributeName);
    var histo = this.getHistogram(attributeName);
    expect(histo.isDisplayed()).toBe(false);
    expect(total.isDisplayed()).toBe(false);
    button.click();
    expect(histo.isDisplayed()).toBe(true);
    expect(total.isDisplayed()).toBe(false);
    function allFrom(td) {
      var toolTip = td.getAttribute('tooltip');
      var style = td.element(by.css('div.bar')).getAttribute('style');
      return testLib.flatten({toolTip: toolTip, style: style}).then(function(rawData) {
        var toolTipMatch = rawData.toolTip.match(/^(.*): (\d+)$/);
        var styleMatch = rawData.style.match(/^height: (\d+)%;$/);
        return {
          title: toolTipMatch[1],
          size: parseInt(styleMatch[1]),
          value: parseInt(toolTipMatch[2]),
        };
      });
    }
    var tds = histo.all(by.css('td'));
    var res = tds.then(function(tds) {
      var res = [];
      for (var i = 0; i < tds.length; i++) {
        res.push(allFrom(tds[i]));
      }
      return protractor.promise.all(res);
    });

    browser.actions().mouseMove(tds.first()).perform();
    expect(total.isDisplayed()).toBe(true);
    testLib.flatten({totalText: total.getText(), values: res}).then(function(c) {
      var totalValue = c.totalText.match(/total: ([0-9,]+)/)[1];
      var total = parseInt(totalValue.replace(/,/g, ''));
      var sum = 0;
      for (var j = 0; j < c.values.length; j++) {
        sum += c.values[j].value;
      }
      expect(total).toEqual(sum);
    });

    button.click();
    expect(histo.isDisplayed()).toBe(false);
    expect(total.isDisplayed()).toBe(false);
    return res;
  },

  getProjectHistory: function() {
    return this.side.element(by.css('div.project.history'));
  },

  getValue: function(id) {
    var asStr = this.side.element(by.css('value#' + id + ' span.value')).getText();
    return asStr.then(function(asS) { return parseInt(asS); });
  },

  getWorkflowCodeEditor: function() {
    return this.side.element(by.id('workflow-code-editor'));
  },

  getWorkflowDescriptionEditor: function() {
    return this.side.element(by.id('workflow-description'));
  },

  getWorkflowNameEditor: function() {
    return this.side.element(by.id('workflow-name'));
  },

  getWorkflowSaveButton: function() {
    return this.side.element(by.id('save-workflow-button'));
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

  openProjectHistory: function() {
    this.side.element(by.css('.history-button')).click();
  },

  openWorkflowSavingDialog: function() {
    this.side.element(by.id('save-as-workflow-button')).click();
  },

  openSegmentation: function(segmentationName) {
    this.side.element(by.id('segmentation-' + segmentationName)).click();
  },

  redoButton: function() {
    return this.side.element(by.id('redo-button'));
  },

  runOperation: function(name, params) {
    params = params || {};
    this.openOperation(name);
    for (var key in params) {
      var p = 'operation-parameters #' + key + ' .operation-attribute-entry';
      testLib.sendKeysToElement(element(by.css(p)), testLib.selectAllKey + params[key]);
    }

    this.toolbox.element(by.css('.ok-button')).click();
  },

  setAttributeFilter: function(attributeName, filterValue) {
    var filterBox = this.side.element(
      by.css('.attribute input[name="' + attributeName + '"]'));
    filterBox.clear();
    filterBox.sendKeys(filterValue, K.ENTER);
  },

  toggleSampledVisualization: function() {
    this.side.element(by.css('label[btn-radio="\'sampled\'"]')).click();
  },

  undoButton: function() {
    return this.side.element(by.id('undo-button'));
  },

  attributeCount: function() {
    return this.side.all(by.css('li.attribute')).count();
  },
};

var visualization = {
  svg: element(by.css('svg.graph-view')),

  // The visualization response received from the server.
  graphView: function() {
    return visualization.svg.evaluate('graph.view.toJSON()');
  },

  vertexCounts: function(index) {
    return visualization.graphView().then(function(gv) {
      return gv.vertexSets[index].vertices.length;
    });
  },
};

var splash = {
  openNewProject: function(name) {
    element(by.id('new-project')).click();
    element(by.id('new-project-name')).sendKeys(name, K.ENTER);
  },

  openProject: function(name) {
    element(by.id('project-' + name)).click();
  },
};

testLib = {
  left: new Side('left'),
  right: new Side('right'),
  visualization: visualization,
  splash: splash,

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

  expectCurrentProjectIs: function(name) {
    expect(browser.getCurrentUrl()).toContain('/#/project/' + name);
  },

  expectHelpPopupVisible: function(helpId, isVisible) {
    expect(element(by.css('div[help-id="' + helpId + '"]')).isDisplayed()).toBe(isVisible);
  },

  // Given an object, finds all promises within it and returns a promise for the completely resolved
  // object.
  flatten: function(objOfPromises) {
    if (typeof objOfPromises !== 'object') {
      return objOfPromises;
    }
    var keys = Object.keys(objOfPromises);
    var promiseKeys = [];
    var result = {};
    for (var i = 0; i < keys.length; i++) {
      var key = keys[i];
      if (typeof objOfPromises[key].then === 'function') {
        promiseKeys.push(key);
      } else {
        result[key] = this.flatten(objOfPromises[key]);
      }
    }

    var elementsSet = 0;
    var defer = protractor.promise.defer();
    function futureSetter(key) {
      return function(value) {
        result[key] = value;
        elementsSet += 1;
        if (elementsSet === promiseKeys.length) {
          defer.fulfill(result);
        }
      };
    }
    for (var j = 0; j < promiseKeys.length; j++) {
      var promiseKey = promiseKeys[j];
      objOfPromises[promiseKey].then(futureSetter(promiseKey));
    }
    return defer;
  },

  openNewProject: function(name) {
    element(by.id('new-project')).click();
    element(by.id('new-project-name')).sendKeys(name, K.ENTER);
  },

  selectAllKey: K.chord(K.CONTROL, 'a'),

  sendKeysToACE: function(e, keys) {
    var aceContent = e.element(by.css('div.ace_content'));
    var aceInput = e.element(by.css('textarea.ace_text-input'));
    // The triple click on the text area focuses it and selects all its
    // text content. Therefore the first key sent will clear its current
    // content. (Caveat: if 'keys' is the empty string then it won't be
    // cleared.)
    browser.actions().doubleClick(aceContent).perform();
    aceInput.sendKeys(keys);
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
            testLib.sendKeysToACE(e, keys);
          }
        });
  },

  // Warning, this also sorts the given array parameter in place.
  sortHistogramValues: function(values) {
    return values.sort(function(b1, b2) {
      if (b1.title < b2.title) {
        return -1;
      } else if (b1.title > b2.title) {
        return 1;
      } else {
        return 0;
      }
    });
  },
};

module.exports = testLib;
