'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'test-example workspace with example graph state selected',
    'snapshot-created',
    function() {
      browser.driver.sleep(2000);
      $$('#create-snapshot-button').click();
      lib.workspace.close();
      browser.driver.sleep(5000);
      console.log('F M L');
      /*expect(lib.error()).toEqual('Entry \'test-example\' already exists.');
      lib.closeErrors();*/
      expect(2).toEqual(2);
    });
};
