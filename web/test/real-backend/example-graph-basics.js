'use strict';

module.exports = function(fw) {
  fw.transitionTest(
    'empty splash',
    'empty test-example project',
    function(lib) {
      lib.openNewProject('test-example');
    },
    function(lib) {
      lib.expectCurrentProjectIs('test-example');
    });
  fw.transitionTest(
    'empty test-example project',
    'test-example project with example graph',
    function(lib) {
      lib.runLeftOperation('example graph');
    },
    function() {
    });
  fw.statePreservingTest(
    'test-example project with example graph',
    'has the proper vertex count',
    function(lib) {
      expect(lib.leftVertexCount()).toEqual(4);
    });
};
