'use strict';

/* global element */
/* global by */

module.exports = function(fw) {
  fw.statePreservingTest(
    'empty test-example project',
    'can open a help popup on the page',
    function() {
      expect(element(by.xpath('//div[@id="project-header-buttons"]')).isDisplayed()).toBeFalsy();
      element(by.xpath('//help-popup[@href="project-header-buttons"]')).click();
      expect(element(by.xpath('//div[@id="project-header-buttons"]')).isDisplayed()).toBeTruthy();
      element(by.xpath('//help-popup[@href="project-header-buttons"]')).click();
      browser.actions().mouseMove({x: 100, y: 100}).perform();
      expect(element(by.xpath('//div[@id="project-header-buttons"]')).isDisplayed()).toBeFalsy();
    });
  fw.transitionTest(
    'test-example project with example graph',
    'test-example project with an opened operation',
    function() {
      expect(element(by.xpath('//operation')).isPresent()).toBeFalsy();
      element(by.xpath('//div[@tooltip="Edge attribute operations"]')).click();
      element(by.xpath('//div[normalize-space(text())="Add constant edge attribute"]')).click();
    },
    function() {
      expect(element(by.xpath('//operation')).isDisplayed()).toBeTruthy();
    });
  fw.statePreservingTest(
    'test-example project with an opened operation',
    'each operation parameter has a help popup icon',
    function() {
      var list = element.all(by.xpath('//operation//operation-parameters/div'));
      expect(list.count()).toBeGreaterThan(0);
      element.all(by.xpath('//operation//operation-parameters/div')).each(
        function(operation) {
          expect(operation.all(by.xpath('.//help-popup')).count()).toBe(1);
          expect(operation.element(by.xpath('.//help-popup')).isDisplayed()).toBeTruthy();
        });
    });
  fw.statePreservingTest(
    'test-example project with example graph',
    'visualization controls have a help icon',
    function() {
      var visualizationEyeIcon = element(by.xpath('//label[@btn-radio="\'sampled\'"]'));
      visualizationEyeIcon.click();
      expect(element(by.xpath('//help-popup[@href="concrete-view-settings"]')).isDisplayed()).toBeTruthy();
      visualizationEyeIcon.click();
    });
};
