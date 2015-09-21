'use strict';

/* global element, by, protractor */

var request = require('request');
var K = protractor.Key;  // Short alias.

function promiseObj(objOfPromises) {
  var keys = Object.keys(objOfPromises);
  var promiseKeys = [];
  var result = {};
  for (var i = 0; i < keys.length; i++) {
    var key = keys[i];
    if (typeof objOfPromises[key].then === 'function') {
      promiseKeys.push(key);
    } else {
      // TODO: Consider doing a recursive call here...
      result[key] = objOfPromises[key];
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
}

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

  getHistogram: function(attributeName) {
    return this.side.element(by.css('histogram[attr-name="' + attributeName + '"]'));
  },

  getHistogramButton: function(attributeName) {
    return this.side.element(by.css('#histogram-button[attr-name="' + attributeName + '"]'));
  },
  
  getHistogramValues: function(attributeName) {
    var button = this.getHistogramButton(attributeName);
    button.click();
    var histo = this.getHistogram(attributeName);
    function allFrom(td) {
      var toolTip = td.getAttribute('tooltip');
      var style = td.element(by.css('div.bar')).getAttribute('style');
      return promiseObj({toolTip: toolTip, style: style}).then(function(rawData) {
        var toolTipMatch = rawData.toolTip.match(/^(.*): (\d+)$/);
        var styleMatch = rawData.style.match(/^height: (\d+)%;$/);
        return {
          title: toolTipMatch[1],
          size: styleMatch[1],
          value: toolTipMatch[2],
        };
      });
    }
    var res = histo.all(by.css('td')).then(function(tds) {
      var res = [];
      for (var i = 0; i < tds.length; i++) {
        res.push(allFrom(tds[i]));
      }
      return protractor.promise.all(res);
    });
    button.click();
    return res;
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
      this.toolbox.element(by.css(p)).sendKeys(params[key]);
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
})();
