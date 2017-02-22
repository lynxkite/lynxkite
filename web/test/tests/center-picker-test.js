'use strict';

var lib = require('../test-lib.js');
var K = protractor.Key;

module.exports = function(fw) {
  var centersToken = lib.left.side.$('#setting-centers');
  var centers = $('#centers');
  var pickButton = $('#pick-and-next-button');
  var pickExpandButton = $('#pick-expand-button');
  var pickOffset = $('#pick-offset');
  var customFiltersToggleOnButton = $('#custom-filters-toggle-on-button');
  var customFiltersToggleOffButton = $('#custom-filters-toggle-off-button');
  var centerCount = $('#pick-center-count');
  var addRestrictionButton = $('#add-restriction-button');


  fw.statePreservingTest(
    'test-example project in sampled view',
    'simple center selection',
    function() {
      // Automatic center selection.
      expect(centersToken.getText()).toBe('centers: 0');
      expect(lib.visualization.vertexCounts(0)).toBe(3);
      centersToken.click();
      expect(pickButton.getText()).toBe('Next');

      // Selection with filter.
      var name = lib.left.vertexAttribute('name');
      name.setFilter('Eve');
      expect(centersToken.getText()).toBe('centers: 0');
      expect(lib.visualization.vertexCounts(0)).toBe(1);
      centersToken.click();
      expect(pickButton.getText()).toBe('Pick');
      pickButton.click();
      expect(lib.visualization.vertexCounts(0)).toBe(1);
      expect(pickButton.getText()).toBe('Next');
      name.setFilter('');

      // Manual center selection.
      centersToken.click();
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
      centersToken.click();
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
      $('#center-restriction-0-attribute').sendKeys('gender');
      $('#center-restriction-0-spec').sendKeys('Male');
      pickButton.click();
      expect(centers.getAttribute('value')).toBe('0, 2');
      pickButton.click();
      expect(centers.getAttribute('value')).toBe('3, 0');

      // Close and discard custom filters.
      lib.expectDialogAndRespond(true);
      customFiltersToggleOffButton.click();
      expect($('#center-restriction-0-attribute').isPresent()).toBe(false);
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
      centersToken.click();
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
