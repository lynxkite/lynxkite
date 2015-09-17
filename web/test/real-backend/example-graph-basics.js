module.exports = function(fw) {
  fw.newUIState(
    'empty test-example project',
    'empty splash',
    function(lib) {
      lib.openNewProject('test-example');
    },
    function(lib) {
      lib.expectCurrentProjectIs('test-example');
    });
  fw.newUIState(
    'test-example project with example graph',
    'empty test-example project',
    function(lib) {
      lib.runOperation('example graph');
    },
    function(lib) {
    });
  fw.newIdempotentTest(
    'test-example project with example graph',
    'has the proper vertex count',
    function(lib) {
      expect(lib.currentVertexCount()).toEqual(4);
    });
};
