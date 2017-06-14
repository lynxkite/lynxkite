'use strict';

module.exports = function () {};

/*
var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'empty splash',
    'can open a help popup on the page by hovering',
    function() {
      lib.setEnablePopups(true);
      lib.splash.openNewProject('test-example');
      var helpIcon = $('help-popup[href="project-header-buttons"]');
      lib.expectNotElement(lib.helpPopup('project-header-buttons'));
      browser.actions().mouseMove(helpIcon).perform();  // hover mouse over icon
      // Popup should open after hover delay.
      lib.wait(function() {
        return lib.helpPopup('project-header-buttons').isPresent();
      });
      browser.actions().mouseMove(lib.left.side, { x: 1, y: 1 }).perform();  // move mouse away
      // Drop is not part of Angular and hiding happens on a short timeout. We need to wait.
      lib.wait(function() {
        return lib.helpPopup('project-header-buttons').isPresent().then(
          function(present) { return !present; });
      });
      lib.setEnablePopups(false);
    },
    function() {
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
      var paramList = $$('operation operation-parameters > div');
      expect(paramList.count()).toBeGreaterThan(0);
      paramList.each(
        function(operation) {
          // Exactly one help popup is displayed.
          expect(operation.all(by.tagName('help-popup')).count()).toBe(1);
          var helpPopup = operation.element(by.tagName('help-popup'));
          expect(helpPopup.isDisplayed()).toBe(true);
          // Check that help id corresponds parameter id, that is:
          // There exists a pair of <div id="<some-parameter-id"> and
          // <help popup="Add-constant-edge-attribute-<some-parameter-id>"> tags.
          operation.$('div.operation-parameter-input').getAttribute('id').then(
            function(parameterId) {
              expect(helpPopup.getAttribute('href')).toBe('Add-constant-edge-attribute-' + parameterId);
            }
          );
        });
    });

  fw.statePreservingTest(
    'test-example project in sampled view',
    'visualization controls have a help icon',
    function() {
      expect($('help-popup[href="concrete-view-settings"]').isDisplayed()).toBe(true);
    });
};
*/
