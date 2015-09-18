'use strict';

/* global element */
/* global by */

var lib = require('./test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'empty test-example project',
    'can open a help popup on the page by clicking',
    function() {
      var helpIcon = element(by.css('help-popup[href="project-header-buttons"]'));
      lib.expectHelpPopupVisible('project-header-buttons', false);
      helpIcon.click();  // click to show popup
      lib.expectHelpPopupVisible('project-header-buttons', true);
      helpIcon.click();  // click to stop showing popup, it still keeps showing :P
      browser.actions().mouseMove({x: 100, y: 100}).perform();  // move mouse away
      lib.expectHelpPopupVisible('project-header-buttons', false);
    });
  fw.statePreservingTest(
    'empty test-example project',
    'can open a help popup on the page by hovering',
    function() {
      var helpIcon = element(by.css('help-popup[href="project-header-buttons"]'));
      lib.expectHelpPopupVisible('project-header-buttons', false);
      browser.actions().mouseMove(helpIcon).perform();  // hover mouse over icon
      lib.expectHelpPopupVisible('project-header-buttons', true);
      browser.actions().mouseMove({x: 100, y: 100}).perform();  // move mouse away
      lib.expectHelpPopupVisible('project-header-buttons', false);
    });
  fw.transitionTest(
    'test-example project with example graph',
    'test-example project with an opened operation',
    function() {
      lib.openLeftOperation('Add constant edge attribute');
    },
    function() {
      expect(element(by.tagName('operation')).isDisplayed()).toBe(true);
    });
  fw.statePreservingTest(
    'test-example project with an opened operation',
    'each operation parameter has a help popup icon',
    function() {
      var paramList = element.all(by.css('operation operation-parameters > div'));
      expect(paramList.count()).toBeGreaterThan(0);
      paramList.each(
        function(operation) {
          expect(operation.all(by.tagName('help-popup')).count()).toBe(1);
          expect(operation.element(by.tagName('help-popup')).isDisplayed()).toBe(true);
        });
    });
  fw.statePreservingTest(
    'test-example project with example graph',
    'visualization controls have a help icon',
    function() {
      lib.toggleLeftSampledVisualization();
      expect(element(by.css('help-popup[href="concrete-view-settings"]')).isDisplayed()).toBe(true);
      lib.toggleLeftSampledVisualization();
    });
};
