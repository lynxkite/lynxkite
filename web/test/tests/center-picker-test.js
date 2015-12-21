'use strict';

/* global protractor, by */
var lib = require('../test-lib.js');
var K = protractor.Key;

module.exports = function(fw) {
  var centers = lib.left.side.element(by.id('centers'));
  var simplePickButton = lib.left.side.element(by.id('simple-pick-button'));
  var advancedPickButton = lib.left.side.element(by.id('advanced-pick-button'));
  var simpleModeButton = lib.left.side.element(by.id('simple-mode-button'));
  var advancedModeButton = lib.left.side.element(by.id('advanced-mode-button'));
  var centerCount = lib.left.side.element(by.id('pick-center-count'));
  var addRestrictionButton = lib.left.side.element(by.id('add-restriction-button'));
  var copyRestrictionsButton = lib.left.side.element(by.id('copy-restrictions-button'));

  fw.statePreservingTest(
    'test-example project in sampled view',
    'simple center selection',
    function() {
      // Automatic center selection.
      expect(centers.getAttribute('value')).toBe('0');
      expect(lib.visualization.vertexCounts(0)).toBe(3);
      expect(simplePickButton.getText()).toBe('Next');

      // Selection with filter.
      lib.left.setAttributeFilter('name', 'Eve');
      expect(centers.getAttribute('value')).toBe('0');
      expect(lib.visualization.vertexCounts(0)).toBe(1);
      expect(simplePickButton.getText()).toBe('Pick');
      simplePickButton.click();
      expect(lib.visualization.vertexCounts(0)).toBe(1);
      expect(simplePickButton.getText()).toBe('Next');
      lib.left.setAttributeFilter('name', '');

      // Manual center selection.
      centers.clear();
      centers.sendKeys('3', K.ENTER); // Isolated Joe.
      expect(lib.visualization.vertexCounts(0)).toBe(1);

      // Next button.
      expect(simplePickButton.getText()).toBe('Pick');
      simplePickButton.click();
      expect(simplePickButton.getText()).toBe('Next');
      expect(centers.getAttribute('value')).toBe('0');
      simplePickButton.click();
      expect(centers.getAttribute('value')).toBe('1');
      simplePickButton.click();
      expect(centers.getAttribute('value')).toBe('2');
      simplePickButton.click();
      expect(centers.getAttribute('value')).toBe('3');
      simplePickButton.click();
      expect(centers.getAttribute('value')).toBe('0');

      // Select "everything".
      centers.clear();
      centers.sendKeys('*', K.ENTER);
      expect(lib.visualization.vertexCounts(0)).toBe(4);

      // Clean up state.
      lib.left.close();
      lib.splash.openProject('test-example');
      lib.left.toggleSampledVisualization();
    });

  fw.statePreservingTest(
    'test-example project in sampled view',
    'advanced center selection',
    function() {
      advancedModeButton.click();

      // Center count.
      centerCount.clear();
      centerCount.sendKeys('2');

      // At this point a tooltip is opened accidentally. It is preventing us from clicking on the pick
      // button. So first we click on the project name to make the tooltip close.
      lib.left.side.element(by.css('div.project-name')).click();
      advancedPickButton.click();
      expect(centers.getAttribute('value')).toBe('0, 1');

      // Next.
      advancedPickButton.click();
      expect(centers.getAttribute('value')).toBe('2, 3');

      // Restriction.
      addRestrictionButton.click();
      lib.left.side.element(by.id('center-restriction-0-attribute')).sendKeys('gender');
      lib.left.side.element(by.id('center-restriction-0-spec')).sendKeys('Male');
      advancedPickButton.click();
      expect(centers.getAttribute('value')).toBe('0, 2');
      advancedPickButton.click();
      expect(centers.getAttribute('value')).toBe('3, 0');

      // Copy restrictions.
      copyRestrictionsButton.click();
      expect(lib.left.side.element(by.id('center-restriction-0-attribute')).isPresent()).toBe(false);

      // Close advanced options.
      simpleModeButton.click();
      expect(simplePickButton.isDisplayed()).toBe(true);

      // Clean up state.
      lib.left.close();
      lib.splash.openProject('test-example');
      lib.left.toggleSampledVisualization();
    }, 'solo');
};
