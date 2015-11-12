'use strict';

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test-example project with example graph',
    'example graph with filters set',
    function() {
      lib.left.setAttributeFilter('name', 'Adam,Eve,Bob');
      lib.left.setAttributeFilter('age', '<40');
      lib.left.setAttributeFilter('weight', '!1');
    },
    function() {
    });

  fw.transitionTest(
    'example graph with filters set',
    'example graph with filters applied',
    function() {
      lib.left.applyFilters();
    },
    function() {
      expect(lib.left.vertexCount()).toEqual(2);
      expect(lib.left.edgeCount()).toEqual(1);
    });
};
