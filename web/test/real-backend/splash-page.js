'use strict';

var lib = require('./test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    undefined,
    'empty splash',
    function() {
      lib.discardAll();
      browser.get('/');
    },
    function() {
    });

  fw.transitionTest(
    'empty splash',
    'empty test-example project',
    function() {
      lib.splash.openNewProject('test-example');
    },
    function() {
      lib.expectCurrentProjectIs('test-example');
    });
};
