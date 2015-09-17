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
      lib.openNewProject('test-example');
    },
    function() {
      lib.expectCurrentProjectIs('test-example');
    });

  fw.transitionTest(
    'empty test-example project',
    'test-example project with example graph',
    function() {
      lib.runLeftOperation('example graph');
    },
    function() {
    });

  fw.statePreservingTest(
    'test-example project with example graph',
    'has the proper vertex count',
    function() {
      expect(lib.leftVertexCount()).toEqual(4);
    });
};
