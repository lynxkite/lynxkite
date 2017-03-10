'use strict';

module.exports = function() {};

/*
var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test-example project with example graph',
    'example graph with filters set',
    function() {
      lib.left.vertexAttribute('name').setFilter('Adam,Eve,Bob');
      lib.left.vertexAttribute('age').setFilter('<40');
      lib.left.edgeAttribute('weight').setFilter('!1');
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
*/
