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
    'test-example workspace with two connected and one independent boxes',
    function() {
      var exampleGraph = lib.workspace.getBox(0);
      lib.workspace.addBox('add reversed edges', 100, 200);
      var reversedEdges = lib.workspace.getBox(1);
      lib.workspace.connectBoxes(
          exampleGraph, 'project',
          reversedEdges, 'project');
      lib.workspace.addBox('create example graph', 350, 100);
    },
    function() {
    });

  fw.transitionTest(
    'test-example workspace with two connected and one independent boxes',
    'test-example workspace with reverse edges state selected',
    function() {
      var reversedEdges = lib.workspace.getBox(1);
      lib.workspace.getOutputPlug(reversedEdges, 'project').click();
    },
    function() {
    });
  
  fw.statePreservingTest(
    'test-example workspace with reverse edges state selected',
    'has the proper vertex count',
    function() {
      expect(lib.state.vertexCount()).toEqual(4);
      expect(lib.state.edgeCount()).toEqual(8);
      expect(lib.state.attributeCount()).toEqual(8);
    });

  fw.statePreservingTest(
    'test-example workspace with reverse edges state selected',
    'states has correct fill color',
    function() {
      let firstExampleGraph = lib.workspace.getBox(0);
      let outPlugFirstEG = lib.workspace.getOutputPlug(firstExampleGraph, 'project');
      let reversedEdges = lib.workspace.getBox(1);
      let outPlugReversedEdges = lib.workspace.getOutputPlug(reversedEdges, 'project');
      let secondExampleGraph = lib.workspace.getBox(2);
      let outPlugSecondEG = lib.workspace.getOutputPlug(secondExampleGraph, 'project');
      expect(outPlugFirstEG.getAttribute('fill')).not.toBeNull();
      expect(outPlugReversedEdges.getAttribute('fill')).not.toBeNull();
      expect(outPlugSecondEG.getAttribute('fill')).toBeNull();
    });

};
