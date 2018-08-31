'use strict';

const lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'empty test-example workspace',
    'example graph workspace',
    function() {
      lib.workspace.addBoxFromSelector('Create example graph');
      lib.workspace.expectNumBoxes(2); // With anchor box
      const K = protractor.Key;
      browser.actions()
        .sendKeys(K.ESCAPE + K.DELETE)
        .perform();
      lib.workspace.expectNumBoxes(1);
    });
};
