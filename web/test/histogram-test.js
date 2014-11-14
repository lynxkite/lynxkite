describe('a histogram', function() {
  beforeEach(function() {
    browser.get('/#/project/Project_Strawberry');
    browser.executeScript('window.runningProtractorTests = true;');
  });

  it('can be opened and closed', function() {
    var attr = element.all(by.css('[vertex-attribute]')).first();
    var button = attr.element(by.css('[tooltip="Show histogram"]'));
    var histogram = attr.element(by.css('histogram'));
    expect(attr.element(by.css('item-name-and-menu > span')).getText()).toBe('age');
    expect(histogram.isDisplayed()).toBe(false);
    button.click();
    expect(histogram.isDisplayed()).toBe(true);
    button.click();
    expect(histogram.isDisplayed()).toBe(false);
  });
});
