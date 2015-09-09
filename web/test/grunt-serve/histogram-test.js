var mocks = require('../mocks.js');
describe('a histogram', function() {
  beforeEach(function() {
    mocks.addTo(browser);
    browser.get('/#/project/Project_Strawberry');
    this.attr = element.all(by.css('[vertex-attribute]')).first();
    this.button = this.attr.element(by.css('[tooltip="Show histogram"]'));
    this.histogram = this.attr.element(by.css('histogram'));
  });

  it('can be opened and closed', function() {
    expect(this.attr.element(by.css('item-name-and-menu > span')).getText()).toBe('age');
    expect(this.histogram.isDisplayed()).toBe(false);
    this.button.click();  // Open.
    expect(this.histogram.isDisplayed()).toBe(true);
    this.button.click();  // Close.
    expect(this.histogram.isDisplayed()).toBe(false);
  });

  it('displays the total on hover', function() {
    this.button.click();  // Open.
    expect(this.histogram.isDisplayed()).toBe(true);
    var total = this.attr.element(by.css('.histogram-total'));
    var td = this.histogram.all(by.css('td')).first();
    expect(total.isDisplayed()).toBe(false);
    browser.actions().mouseMove(td).perform();
    expect(total.isDisplayed()).toBe(true);
    expect(total.getText()).toBe('total: 3,330');
  });
});
