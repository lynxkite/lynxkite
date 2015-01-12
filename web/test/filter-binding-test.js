var mocks = require('./mocks.js');
describe('the all filters button', function() {
  beforeEach(function() {
    mocks.addTo(browser);
    browser.get('/#/project/Project_Strawberry');
    this.header = element(by.css('.project-name'))
    //this.button = this.attr.element(by.css('[tooltip="Show histogram"]'));
    //this.histogram = this.attr.element(by.css('histogram'));
  });

  it('is linked to the vertex attribute filter box', function() {
    expect(this.header.all(by.css('[ng-click="side.applyFilters()"]')).count())
      .toBe(0);
    element.all(by.css('[vertex-attribute] input[placeholder="filter"]')).first().sendKeys('a\n');
    expect(this.header.all(by.css('[ng-click="side.applyFilters()"]')).count())
      .toBe(1);
  });
  it('is linked to the edge attribute filter box', function() {
    expect(this.header.all(by.css('[ng-click="side.applyFilters()"]')).count())
      .toBe(0);
    element.all(by.css('[edge-attribute] input[placeholder="filter"]')).first().sendKeys('a\n');
    expect(this.header.all(by.css('[ng-click="side.applyFilters()"]')).count())
      .toBe(1);
  });
  it('is linked to the segmentation filter box', function() {
    expect(this.header.all(by.css('[ng-click="side.applyFilters()"]')).count())
      .toBe(0);
    element.all(by.css('[segmentation] input[placeholder="filter"]')).first().sendKeys('a\n');
    expect(this.header.all(by.css('[ng-click="side.applyFilters()"]')).count())
      .toBe(1);
  });
});
