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
    'test-example workspace with two connected and one unconnected box',
    function() {
      var exampleGraph = lib.workspace.getBox(0);
      lib.workspace.addBox('add reversed edges', 100, 200);
      var reversedEdges = lib.workspace.getBox(1);
      lib.workspace.connectBoxes(
          exampleGraph, 'project',
          reversedEdges, 'project');
      lib.workspace.addBox('compute pagerank', 350, 100);
    },
    function() {
    });

  fw.statePreservingTest(
    'test-example workspace with two connected and one unconnected box',
    'states has correct fill color',
    function() {
      let exampleGraph = lib.workspace.getBox(0);
      let outPlugEG = lib.workspace.getOutputPlug(exampleGraph, 'project');
      let reversedEdges = lib.workspace.getBox(1);
      let outPlugReversedEdges = lib.workspace.getOutputPlug(reversedEdges, 'project');
      let pageRank = lib.workspace.getBox(2);
      let outPlugPR = lib.workspace.getOutputPlug(pageRank, 'project');
      // progress is updated every 2 seconds, so we may need to wait
      browser.wait(() => outPlugReversedEdges.getAttribute('fill'), 1000 * 3);
      expect(outPlugEG.getAttribute('fill')).not.toBeNull();
      expect(outPlugPR.getAttribute('fill')).toBeNull();
    });

  fw.transitionTest(
    'test-example workspace with two connected and one unconnected box',
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

};
