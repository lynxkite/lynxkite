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
  var saveVisualizationOpen = lib.left.side.$('#save-visualization-dialog #text-dialog-open');
  var saveVisualizationEntry = lib.left.side.$('#save-visualization-dialog #dialogInput');
  var saveVisualizationOk = lib.left.side.$('#save-visualization-dialog #text-dialog-ok');

  fw.statePreservingTest(
    'test-example project in sampled view',
    'simple center selection',
    function() {
      // Automatic center selection.
      expect(centers.getAttribute('value')).toBe('0');
      expect(lib.visualization.vertexCounts(0)).toBe(3);
      expect(pickButton.getText()).toBe('Next');

      // Selection with filter.
      var name = lib.left.vertexAttribute('name');
      name.setFilter('Eve');
      expect(centers.getAttribute('value')).toBe('0');
      expect(lib.visualization.vertexCounts(0)).toBe(1);
      expect(pickButton.getText()).toBe('Pick');
      pickButton.click();
      expect(lib.visualization.vertexCounts(0)).toBe(1);
      expect(pickButton.getText()).toBe('Next');
      name.setFilter('');

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

  fw.transitionTest(
    'test-example project with example graph',
    'visualization save/restore',
    function() {
      lib.left.toggleSampledVisualization();
      // Set centers count to a non-default value.
      centerCount.clear();
      centerCount.sendKeys('2');
      pickButton.click();
      // Save the visualization with the name 'my visualization'
      saveVisualizationOpen.click();
      saveVisualizationEntry.clear();
      saveVisualizationEntry.sendKeys('my visualization');
      saveVisualizationOk.click();
      // Close and reopen the project and check if the eye icon is still there. issues/#3164
      lib.left.close();
      lib.splash.openProject('test-example');
      // Try loading the visualization and check if centers count is correctly updated.
      lib.left.toggleSampledVisualization();
      expect(centerCount.getAttribute('value')).toBe('1');
      lib.left.scalar('my-visualization').clickMenu('load-visualization');
      expect(centerCount.getAttribute('value')).toBe('2');
    },
    function() {});
};
