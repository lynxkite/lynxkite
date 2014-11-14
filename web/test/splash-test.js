describe('splash page', function() {
  beforeEach(function() {
    browser.get('/');
    browser.executeScript('window.runningProtractorTests = true;');
  });
  it('has a title', function() {
    expect(browser.getTitle()).toEqual('Lynx PizzaKite');
  });
  describe('project selector', function() {
    it('opens a project when clicked', function() {
      var melon = element(by.partialLinkText('Project Melon'));
      melon.click();
      expect(element(by.css(".project-sides")).isPresent()).toBeTruthy();
      expect(browser.getCurrentUrl()).toContain('/#/project/Project_Melon');
    });
  });
});
