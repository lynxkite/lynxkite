var mocks = require('../mocks.js');
describe('splash page', function() {
  beforeEach(function() {
    mocks.addTo(browser);
    browser.get('/');
  });

  it('has a title', function() {
    expect(browser.getTitle()).toEqual('LynxKite');
  });

  it('has a version string', function() {
    expect(element(by.css('.version')).getText()).toBe('Static Ajax Test');
  });

  describe('project selector', function() {
    it('opens a project when clicked', function() {
      var melon = element.all(by.css('.project-list > .project-entry')).get(1);
      melon.click();
      expect(browser.getCurrentUrl()).toContain('/#/project/Project_Melon');
    });
  });
});
