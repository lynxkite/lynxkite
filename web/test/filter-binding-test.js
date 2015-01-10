var mocks = require('./mocks.js');
describe('the all filters button', function() {
  beforeEach(function() {
    mocks.addTo(browser);
    browser.get('/#/project/Project_Strawberry');
    this.header = element.all(by.css('.project-name')).first();
    //this.button = this.attr.element(by.css('[tooltip="Show histogram"]'));
    //this.histogram = this.attr.element(by.css('histogram'));
  });

  it('is linked to the segmentation filter box', function() {
    expect(this.header.element(by.css('[ng-click="side.applyFilters()"]'))).toBe(undefined)

/*    expect(this.attr.element(by.css('item-name-and-menu > span')).getText()).toBe('age');
    expect(this.histogram.isDisplayed()).toBe(false);
    this.button.click();  // Open.
    expect(this.histogram.isDisplayed()).toBe(true);
    this.button.click();  // Close.
    expect(this.histogram.isDisplayed()).toBe(false);*/
  });
});
