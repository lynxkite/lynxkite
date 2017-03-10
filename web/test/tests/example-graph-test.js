'use strict';

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'empty test-example workspace',
    'test-example workspace with example graph',
    function() {
      lib.workspace.addBox('create example graph', 100, 100);
    },
    function() {
    });

  fw.transitionTest(
    'test-example workspace with example graph',
    'test-example workspace with example graph state selected',
    function() {
      lib.workspace.getOutputPlug(0, 'project').click();
    },
    function() {
    });

  fw.statePreservingTest(
    'test-example workspace with example graph state selected',
    'has the proper vertex count',
    function() {
      expect(lib.state.vertexCount()).toEqual(4);
      expect(lib.state.edgeCount()).toEqual(4);
      expect(lib.state.attributeCount()).toEqual(8);
    });

  fw.transitionTest(
    'test-example workspace with example graph',
    'test-example workspace with two connected boxes',
    function() {
      lib.workspace.addBox('add reversed edges', 100, 200);
      lib.workspace.connectBoxes(0, 'project', 1, 'project');
    },
    function() {
    });

  fw.transitionTest(
    'test-example workspace with two connected boxes',
    'has the proper vertex count',
    function() {
      lib.workspace.getOutputPlug(1, 'project').click();
      expect(lib.state.vertexCount()).toEqual(4);
      expect(lib.state.edgeCount()).toEqual(8);
      expect(lib.state.attributeCount()).toEqual(8);
    },
    function() {
    });

};
