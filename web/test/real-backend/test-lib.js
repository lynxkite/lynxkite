'use strict';

/* global element, by, protractor */

var testLib; // Forward declarations.
var History; // Forward declarations.
var request = require('request');
var K = protractor.Key;  // Short alias.

function Side(direction) {
  this.direction = direction;
  this.side = element(by.id('side-' + direction));
  this.toolbox = element(by.id('operation-toolbox-' + direction));
  this.history = new History(this);
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

  openWorkflowSavingDialog: function() {
    this.side.element(by.id('save-as-workflow-button')).click();
  },

  openSegmentation: function(segmentationName) {
    this.side.element(by.id('segmentation-' + segmentationName)).click();
  },

  redoButton: function() {
    return this.side.element(by.id('redo-button'));
  },

  populateOperation: function(parentElement, params) {
    params = params || {};
    for (var key in params) {
      var p = 'operation-parameters #' + key + ' .operation-attribute-entry';
      testLib.sendKeysToElement(
          parentElement.element(by.css(p)),
          testLib.selectAllKey + params[key]);
    }
  },

  submitOperation: function(parentElement) {
    parentElement.element(by.css('.ok-button')).click();
  },

  runOperation: function(name, params) {
    this.openOperation(name);
    this.populateOperation(this.toolbox, params);
    this.submitOperation(this.toolbox);
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

function History(side) {
  this.side = side;
}

History.prototype = {
  open: function() {
    this.side.side.element(by.css('.history-button')).click();
  },

  close: function(discardChanges) {
    if (discardChanges) {
      testLib.expectDialogAndRespond(true);
    }
    this.side.side.element(by.id('close-history-button')).click();
    if (discardChanges) {
      testLib.checkAndCleanupDialogExpectation();
    }
  },

  save: function(name) {
    this.side.side.element(by.css('.save-history-button')).click();
    if (name !== undefined) {
      var inputBox = this.side.side.element(by.css('.save-as-history-box input'));
      inputBox.sendKeys(testLib.selectAllKey + name);
    }
    this.side.side.element(by.css('.save-as-history-box .glyphicon-floppy-disk')).click();
  },

  expectSaveable: function(saveable) {
    expect(this.side.side.element(by.css('.save-history-button')).isPresent()).toBe(saveable);
  },

  // Get an operation from the history. position is a zero-based index.
  getOperation: function(position) {
    var list = this.side.side.all(by.css('project-history div.list-group > li'));
    return list.get(position);
  },

  getOperationName: function(position) {
    return this.getOperation(position).element(by.css('h1')).getText();
  },

  openDropdownMenu: function(operation) {
    var menu = operation.element(by.css('.history-options.dropdown'));
    menu.element(by.css('a.dropdown-toggle')).click();
    return menu;
  },

  deleteOperation: function(position) {
    var operation = this.getOperation(position);
    this.openDropdownMenu(operation).element(by.css('ul .glyphicon-trash')).click();
  },

  addOperation: function(parentPos, direction, name, params) {
    var parentOp = this.getOperation(parentPos);
    var iconClass = '.glyphicon-chevron-' + direction;
    this.openDropdownMenu(parentOp).element(by.css('ul ' + iconClass)).click();
    var newPos = direction === 'up' ? parentPos : parentPos + 1;
    var newOp = this.getOperation(newPos);
    newOp.element(by.id('operation-search')).click();
    newOp.element(by.id('filter')).sendKeys(name, K.ENTER);
    this.side.populateOperation(newOp, params);
    this.side.submitOperation(newOp);
  },

  numOperations: function() {
    return this.side.side.all(by.css('project-history div.list-group > li')).count();
  },

  expectOperationParameter: function(opPosition, paramName, expectedValue) {
    var param = this.getOperation(opPosition).element(by.css('div#' + paramName + ' input'));
    expect(param.getAttribute('value')).toBe(expectedValue);
  }

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
    this.hideSparkStatus();
  },

  openProject: function(name) {
    element(by.id('project-' + name)).click();
    this.hideSparkStatus();
  },

  hideSparkStatus: function() {
    // Floating elements can overlap buttons and block clicks.
    browser.executeScript(
      'document.styleSheets[0].insertRule(\'.spark-status { position: static !important; }\');');
  },
};

testLib = {
  left: new Side('left'),
  right: new Side('right'),
  visualization: visualization,
  splash: splash,
  selectAllKey: K.chord(K.CONTROL, 'a'),

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

  sendKeysToACE: function(e, keys) {
    var aceContent = e.element(by.css('div.ace_content'));
    var aceInput = e.element(by.css('textarea.ace_text-input'));
    // The double click on the text area focuses it properly.
    browser.actions().doubleClick(aceContent).perform();
    aceInput.sendKeys(keys);
  },

  sendKeysToElement: function(e, keys) {
    // ACE editor and non-ace controls need different handling.
    e.evaluate('param.kind').then(
        function(dataKind) {
          if (dataKind === 'code') {
            testLib.sendKeysToACE(e, keys);
          } else {
            e.sendKeys(keys);
          }
        });
  },

  // Expects a window.confirm call from the client code and overrides the user
  // response.
  expectDialogAndRespond: function(responseValue) {
    // I am not particularly happy with this solution. The problem with the nice
    // solution is that there is a short delay before the alert actually shows up
    // and protractor does not wait for it. (Error: NoSuchAlertError: no alert open)
    // See: https://github.com/angular/protractor/issues/1486
    // Other possible options:
    // 1. browser.wait for the alert to appear. This introduces a hard timout
    // and potential flakiness.
    // 2. Use Jasmine's spyOn. The difficulty there is in getting hold of a
    // window object from inside the browser, if at all ppossible.
    // 3. Use a mockable Angular module for window.confirm from our app.
    browser.executeScript(
        'window.confirm0 = window.confirm;' +
        'window.confirm = function() {' +
        '  window.confirm = window.confirm0;' +
        '  return ' + responseValue+ ';' +
        '}');
  },

  checkAndCleanupDialogExpectation: function() {
    // Fail if there was no alert.
    expect(browser.executeScript('return window.confirm === window.confirm0')).toBe(true);
    browser.executeScript('window.confirm = window.confirm0;');
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
