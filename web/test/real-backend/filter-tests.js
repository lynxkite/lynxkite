'use strict';

var lib = require('./test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test-example project with example graph',
    'example graph with filters set',
    function() {
      lib.setLeftAttributeFilter('name', 'Adam,Eve,Bob');
      lib.setLeftAttributeFilter('age', '<40');
      lib.setLeftAttributeFilter('weight', '!1');
    },
    function() {
    });
  fw.transitionTest(
    'example graph with filters set',
    'example graph with filters applied',
    function() {
      lib.leftApplyFilters();
    },
    function() {
      expect(lib.leftVertexCount()).toEqual(2);
      expect(lib.leftEdgeCount()).toEqual(1);
    });
};
