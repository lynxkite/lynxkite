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
    'test-example project with the "Add constant edge attribute" opened',
    function() {
      lib.left.openOperation('Add constant edge attribute');
    },
    function() {
      expect(element(by.tagName('operation')).isDisplayed()).toBe(true);
    });
  fw.statePreservingTest(
    'test-example project with the "Add constant edge attribute" opened',
    'each operation parameter has a help popup icon',
    function() {
      var paramList = element.all(by.css('operation operation-parameters > div'));
      expect(paramList.count()).toBeGreaterThan(0);
      paramList.each(
        function(operation) {
          // Exactly one help popup is displayed.
          expect(operation.all(by.tagName('help-popup')).count()).toBe(1);
          var helpPopup = operation.element(by.tagName('help-popup'));
          expect(helpPopup.isDisplayed()).toBe(true);
          // Check that help id corresponds attribute id, that is:
          // There exists a pair of <div id="<some-operation-id"> and
          // <help popup="Add-constant-edge-attribute-<some-operation-id>"> tags.
          operation.element(by.css('div.operation-attribute-input')).getAttribute('id').then(
            function(attributeId) {
              expect(helpPopup.getAttribute('href')).toBe('Add-constant-edge-attribute-' + attributeId);
            }
          );
        });
    });
  fw.statePreservingTest(
    'test-example project with example graph',
    'visualization controls have a help icon',
    function() {
      lib.left.toggleSampledVisualization();
      expect(element(by.css('help-popup[href="concrete-view-settings"]')).isDisplayed()).toBe(true);
      lib.left.toggleSampledVisualization();
    });
};
