'use strict';

/* global element, by */
var lib = require('./test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'empty test-example project',
    'test-example project with example graph',
    function() {
      lib.left.runOperation('example graph');
    },
    function() {
    });

  fw.statePreservingTest(
    'test-example project with example graph',
    'has the proper vertex count',
    function() {
      expect(lib.left.vertexCount()).toEqual(4);
      expect(lib.left.edgeCount()).toEqual(4);
      expect(lib.left.attributeCount()).toEqual(8);
    });

  fw.transitionTest(
    'test-example project with example graph',
    'test-example project in sampled view',
    function() {
      lib.left.toggleSampledVisualization();
    },
    function() {
      expect(element(by.css('svg.graph-view')).isDisplayed()).toBe(true);
    });
};
