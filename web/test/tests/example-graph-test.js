'use strict';

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'empty test-example workspace',
    'test-example workspace with example graph',
    function() {
      lib.workspace.addBox({
        id: 'first-example-graph',
        name: 'create example graph',
        x: 100,
        y: 100,
      });
    },
    function() {
    });

  fw.transitionTest(
    'test-example workspace with example graph',
    'test-example workspace with example graph state selected',
    function() {
      lib.workspace.getOutputPlug('first-example-graph', 'project').click();
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
      lib.workspace.addBox({
        id: 'reversed-edges',
        name: 'add reversed edges',
        x: 100,
        y: 200,
        inputs: [{
          boxID: 'first-example-graph',
          srcPlugID: 'project',
          dstPlugID: 'project'
        }]
      });
      lib.workspace.addBox({
        id: 'second-example-graph',
        name: 'create example graph',
        x: 350,
        y: 100
      });
    },
    function() {
    });

  fw.transitionTest(
    'test-example workspace with two connected and one independent boxes',
    'test-example workspace with reverse edges state selected',
    function() {
      lib.workspace.getOutputPlug('reversed-edges', 'project').click();
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
      let outPlugFirstEG = lib.workspace.getOutputPlug('first-example-graph', 'project');
      let outPlugReversedEdges = lib.workspace.getOutputPlug('reversed-edges', 'project');
      let outPlugSecondEG = lib.workspace.getOutputPlug('second-example-graph', 'project');
      // progress is updated every 2 seconds, so we may need to wait
      browser.wait(() => outPlugReversedEdges.getAttribute('fill'), 1000 * 3);
      expect(outPlugFirstEG.getAttribute('fill')).not.toBeNull();
      expect(outPlugSecondEG.getAttribute('fill')).toBeNull();
    });

};
