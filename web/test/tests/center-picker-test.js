'use strict';

var lib = require('../test-lib.js');
var K = protractor.Key;

module.exports = function(fw) {
  var centers = lib.left.side.element(by.id('centers'));
  var pickButton = lib.left.side.element(by.id('pick-and-next-button'));
  var pickExpandButton = lib.left.side.element(by.id('pick-expand-button'));
  var pickOffset = lib.left.side.element(by.id('pick-offset'));
  var customFiltersToggleOnButton =
    lib.left.side.element(by.id('custom-filters-toggle-on-button'));
  var customFiltersToggleOffButton =
    lib.left.side.element(by.id('custom-filters-toggle-off-button'));
  var centerCount = lib.left.side.element(by.id('pick-center-count'));
  var addRestrictionButton = lib.left.side.element(by.id('add-restriction-button'));


  fw.statePreservingTest(
    'test-example project in sampled view',
    'simple center selection',
    function() {
      // Automatic center selection.
      expect(centers.getAttribute('value')).toBe('0');
      expect(lib.visualization.vertexCounts(0)).toBe(3);
      expect(pickButton.getText()).toBe('Next');

      // Selection with filter.
      lib.left.setAttributeFilter('name', 'Eve');
      expect(centers.getAttribute('value')).toBe('0');
      expect(lib.visualization.vertexCounts(0)).toBe(1);
      expect(pickButton.getText()).toBe('Pick');
      pickButton.click();
      expect(lib.visualization.vertexCounts(0)).toBe(1);
      expect(pickButton.getText()).toBe('Next');
      lib.left.setAttributeFilter('name', '');

      // Manual center selection.
      centers.clear();
      centers.sendKeys('3', K.ENTER); // Isolated Joe.
      expect(lib.visualization.vertexCounts(0)).toBe(1);

      // Next button.
      expect(pickButton.getText()).toBe('Pick');
      pickButton.click();
      expect(pickButton.getText()).toBe('Next');
      expect(centers.getAttribute('value')).toBe('0');
      pickButton.click();
      expect(centers.getAttribute('value')).toBe('1');
      pickButton.click();
      expect(centers.getAttribute('value')).toBe('2');
      pickButton.click();
      expect(centers.getAttribute('value')).toBe('3');
      pickButton.click();
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
      customFiltersToggleOnButton.click();

      // Center count.
      centerCount.clear();
      centerCount.sendKeys('2');
      pickButton.click();
      expect(centers.getAttribute('value')).toBe('0, 1');

      // Next.
      pickButton.click();
      expect(centers.getAttribute('value')).toBe('2, 3');

      // Restriction.
      addRestrictionButton.click();
      lib.left.side.element(by.id('center-restriction-0-attribute')).sendKeys('gender');
      lib.left.side.element(by.id('center-restriction-0-spec')).sendKeys('Male');
      pickButton.click();
      expect(centers.getAttribute('value')).toBe('0, 2');
      pickButton.click();
      expect(centers.getAttribute('value')).toBe('3, 0');

      // Close and discard custom filters.
      lib.expectDialogAndRespond(true);
      customFiltersToggleOffButton.click();
      expect(lib.left.side.element(by.id('center-restriction-0-attribute')).isPresent()).toBe(false);
      expect(addRestrictionButton.isDisplayed()).toBe(false);

      // Clean up state.
      lib.left.close();
      lib.splash.openProject('test-example');
      lib.left.toggleSampledVisualization();
    });

  fw.statePreservingTest(
    'test-example project in sampled view',
    'pick by offset',
    function() {
      centerCount.clear();
      centerCount.sendKeys('2');

      pickButton.click();
      pickButton.click();
      pickButton.click();
      pickExpandButton.click();
      expect(pickOffset.getAttribute('value')).toBe('4');
      pickOffset.clear();
      pickOffset.sendKeys('40');
      pickButton.click();
      pickButton.click();
      expect(pickOffset.getAttribute('value')).toBe('42');
      pickExpandButton.click();

      // Restore original state.
      centerCount.clear();
      centerCount.sendKeys('1');
    });
};
