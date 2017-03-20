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
      var box = lib.workspace.getBox(0);
      lib.workspace.getOutputPlug(box, 'project').click();
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
      var exampleGraph = lib.workspace.getBox(0);
      lib.workspace.addBox('add reversed edges', 100, 200);
      var reversedEdges = lib.workspace.getBox(1);
      lib.workspace.connectBoxes(
          exampleGraph, 'project',
          reversedEdges, 'project');
    },
    function() {
    });

  fw.transitionTest(
    'test-example workspace with two connected boxes',
    'has the proper vertex count',
    function() {
      var reversedEdges = lib.workspace.getBox(1);
      lib.workspace.getOutputPlug(reversedEdges, 'project').click();
      expect(lib.state.vertexCount()).toEqual(4);
      expect(lib.state.edgeCount()).toEqual(8);
      expect(lib.state.attributeCount()).toEqual(8);
    },
    function() {
    });

};
