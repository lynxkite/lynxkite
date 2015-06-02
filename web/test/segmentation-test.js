var mocks = require('./mocks.js');
describe('the segmentation', function() {
  beforeEach(function() {
    mocks.addTo(browser);
    browser.get('/#/project/Project_Strawberry');
    this.sides = element.all(by.css('div.project'));
    this.segmentation = this.sides.first().all(by.css('[segmentation]')).first();
    this.openButton = this.segmentation.element(by.css('[tooltip="Open"]'));
  });

  it('can be opened', function() {
    var name = this.segmentation.element(by.css('item-name-and-menu > span')).getText();
    expect(name).toBe('maximal_cliques');
    expect(this.sides.count()).toBe(1);
    this.openButton.click();
    expect(this.sides.count()).toBe(2);
    function projectName(p) {
      return p.element(by.css('.project-name')).getText();
    }
    expect(projectName(this.sides.get(0))).toBe('Project Strawberry');
    expect(projectName(this.sides.get(1))).toContain('maximal cliques');
  });
});
