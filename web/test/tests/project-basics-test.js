'use strict';

module.exports = function() {};

/*
var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test-example project with example graph',
    'test-example project with example graph saved as apple',
    function() {
      lib.left.saveProjectAs('apple');
    },
    function() {
      // We are now in a project with the new name.
      lib.left.expectCurrentProjectIs('apple');
      // We also kept the contents of the project.
      expect(lib.left.vertexCount()).toEqual(4);
      expect(lib.left.edgeCount()).toEqual(4);
      expect(lib.left.attributeCount()).toEqual(8);
    });

  fw.transitionTest(
    'test-example project with example graph saved as apple',
    'test-example project with example graph saved as pear/apple',
    function() {
      lib.left.saveProjectAs('pear/apple');
    },
    function() {
      // We are now in a project with the new name.
      lib.left.expectCurrentProjectIs('pear/apple');
      // We also kept the contents of the project.
      expect(lib.left.vertexCount()).toEqual(4);
      expect(lib.left.edgeCount()).toEqual(4);
      expect(lib.left.attributeCount()).toEqual(8);
    });

  fw.statePreservingTest(
    'test-example project with example graph saved as apple',
    'cant save as to existing project',
    function() {
      lib.left.saveProjectAs('test-example');
      expect(lib.error()).toEqual('Entry \'test-example\' already exists.');
      lib.closeErrors();
    });
};
*/
