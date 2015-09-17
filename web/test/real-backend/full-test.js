'use strict';

/* global element */
/* global by */
/* global protractor */

var mocks = require('../mocks.js');
describe('We can', function() {
  var K = protractor.Key;  // Short alias.
  mocks.addTo(browser);

  // These tests are run in a sequence, building on the previous steps.
  it('open LynxKite', function() {
    browser.get('/');
  });

  function discardDirectory(dir) {
    element(by.css('#directory-' + dir + ' .btn-dropdown')).click();
    element(by.css('#directory-' + dir + ' #menu-discard')).click();
    var confirmation = browser.switchTo().alert();
    expect(confirmation.getText()).toContain('delete directory test folder?');
    confirmation.accept();
  }

  it('delete the directory if it exists', function() {
    element(by.id('directory-test_folder')).isPresent().then(function(exists) {
      if (exists) {
        discardDirectory('test_folder');
      }
    });
  });

  it('create a new directory', function() {
    element(by.id('new-directory')).click();
    element(by.id('new-directory-name')).sendKeys('test_folder', K.ENTER);
    expect(element(by.id('current-directory')).getText()).toContain('test_folder');
  });

  it('create a new project', function() {
    element(by.id('new-project')).click();
    element(by.id('new-project-name')).sendKeys('test_project', K.ENTER);
    expect(browser.getCurrentUrl()).toContain('/#/project/test_folder/test_project');
  });

  function runOperation(name, andThen) {
    it('run ' + name, function() {
      element(by.id('operation-search')).click();
      element(by.css('operation-toolbox #filter')).sendKeys(name, K.ENTER);
      element(by.css('operation-toolbox .ok-button')).click();
      andThen();
    });
  }

  runOperation('example graph', function() {
    expect(element(by.id('side-summary')).getText()).toContain('4 vertices with 4 edges');
  });

  runOperation('connected components', function() {
    element(by.id('segmentation-connected_components')).click();
    expect(element(by.css('#side-right #side-summary')).getText())
      .toContain('2 non-empty segments with total size of 4 covering 4 base vertices');
  });

  function expectVertexCount(n) {
    expect(element.all(by.css('.graph-view g.nodes g.vertex')).count()).toBe(n);
  }

  it('look at graph', function() {
    element(by.css('#side-left #bucketed-mode-button')).click();
    expectVertexCount(1);
    element(by.css('#side-right #sampled-mode-button')).click();
    expectVertexCount(2);
    element(by.css('#side-right #centers')).sendKeys(K.chord(K.CONTROL, 'a'), '*', K.ENTER);
    expectVertexCount(3);
    element(by.id('axis-y-gender')).click();
    expectVertexCount(4);
  });

  it('clean up', function() {
    element(by.css('#side-right #close-project')).click();
    element(by.css('#side-left #close-project')).click();
    discardDirectory('test_folder');
    expect(element(by.id('directory-test_folder')).isPresent()).toBe(false);
  });
});
