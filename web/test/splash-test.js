describe('splash page', function() {
  beforeEach(function() {
    browser.get('/');
    browser.executeScript('window.runningProtractorTests = true;');
  });

  it('has a title', function() {
    expect(browser.getTitle()).toEqual('Lynx PizzaKite');
  });

  it('has a version string', function() {
    expect(element(by.css('.version')).getText()).toBe('Static Ajax Test');
  });

  describe('project selector', function() {
    it('can search', function() {
      var list = element.all(by.css('.project-list h1'));
      expect(list.count()).toBe(3);
      element(by.css('[placeholder="type to search"]')).sendKeys('v');
      expect(list.count()).toBe(1);
      expect(list.first().getText()).toBe('Project Avocado');
    });

    it('opens a project when clicked', function() {
      var melon = element(by.partialLinkText('Project Melon'));
      melon.click();
      expect(browser.getCurrentUrl()).toContain('/#/project/Project_Melon');
    });
  });
});
