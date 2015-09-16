var lib = require('./test-lib.js');
describe('Example workflow', function() {
  lib.initTest();
  it('can count the vertices of example graph', function() {
    lib.startPage();
    lib.openNewProject('test-example');
    lib.runOperation('example graph');
    expect(lib.currentVertexCount()).toEqual(4);
  });
})
