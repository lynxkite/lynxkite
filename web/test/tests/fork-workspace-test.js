'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'empty test-example workspace',
    'empty test-example workspace forked',
    function() {
      $('#save-workspace-as-starter-button').click();
      lib.submitInlineInput($('#save-workspace-as-input'), 'test-example-fork');
    },
    function() {
      expect(browser.getCurrentUrl()).toEqual(browser.baseUrl + '#/workspace/test-example-fork');
      lib.workspace.close();
      lib.splash.expectWorkspaceListed('test-example-fork');
    });
};
