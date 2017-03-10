'use strict';

module.exports = function() {};

/*
var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'example graph with filters applied',
    'after new operation we can undo but not redo',
    function() {
      expect(lib.left.undoButton().getAttribute('disabled')).toBeFalsy();
      expect(lib.left.redoButton().getAttribute('disabled')).toBeTruthy();
    });
  fw.transitionTest(
    'example graph with filters applied',
    'example graph with filters undone',
    function() {
      lib.left.undoButton().click();
    },
    function() {
      expect(lib.left.undoButton().getAttribute('disabled')).toBeFalsy();
      expect(lib.left.redoButton().getAttribute('disabled')).toBeFalsy();
      expect(lib.left.vertexCount()).toEqual(4);
      expect(lib.left.attributeCount()).toEqual(8);
    });
  fw.transitionTest(
    'example graph with filters undone',
    'example graph with filters undone undone',
    function() {
      lib.left.undoButton().click();
    },
    function() {
      expect(lib.left.undoButton().getAttribute('disabled')).toBeTruthy();
      expect(lib.left.redoButton().getAttribute('disabled')).toBeFalsy();
      expect(lib.left.attributeCount()).toEqual(0);
    });
  fw.transitionTest(
    'example graph with filters undone undone',
    'example graph with filters undone undone redone',
    function() {
      lib.left.redoButton().click();
    },
    function() {
      expect(lib.left.undoButton().getAttribute('disabled')).toBeFalsy();
      expect(lib.left.redoButton().getAttribute('disabled')).toBeFalsy();
      expect(lib.left.vertexCount()).toEqual(4);
      expect(lib.left.attributeCount()).toEqual(8);
    });
  fw.transitionTest(
    'example graph with filters undone undone redone',
    'example graph with filters undone undone redone redone',
    function() {
      lib.left.redoButton().click();
    },
    function() {
      expect(lib.left.undoButton().getAttribute('disabled')).toBeFalsy();
      expect(lib.left.redoButton().getAttribute('disabled')).toBeTruthy();
      expect(lib.left.vertexCount()).toEqual(2);
      expect(lib.left.attributeCount()).toEqual(8);
    });
};
*/
