'use strict';

/* global element, by, protractor */

var request = require('request');

module.exports = (function() {
  var K = protractor.Key;  // Short alias.

  var ToolTipRE = new RegExp('^(.*): (\\d+)$');
  var StyleRE = new RegExp('^height: (\\d+)%;$');

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
      function allFrom(td) {
        var toolTip = td.getAttribute('tooltip');
        var style = td.element(by.css('div.bar')).getAttribute('style');
        return promiseObj({toolTip: toolTip, style: style}).then(function(rawData) {
          var toolTipMatch = rawData.toolTip.match(ToolTipRE);
          var styleMatch = rawData.style.match(StyleRE);
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
