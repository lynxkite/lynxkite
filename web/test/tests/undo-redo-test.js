'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {
  var undo = $('#undo');
  var redo = $('#redo');
  fw.transitionTest(
    'delete boxes',
    'delete undone',
    function() {
      expect(lib.workspace.boxExists('ex0')).toEqual(false);
      expect(lib.workspace.boxExists('pr1')).toEqual(false);
      expect(lib.workspace.boxExists('pr2')).toEqual(true);
      expect(undo.getAttribute('disabled')).toBeFalsy();
      expect(redo.getAttribute('disabled')).toBeTruthy();
      undo.click();
    },
    function() {
      expect(undo.getAttribute('disabled')).toBeFalsy();
      expect(redo.getAttribute('disabled')).toBeFalsy();
      expect(lib.workspace.boxExists('ex0')).toEqual(true);
      expect(lib.workspace.boxExists('pr1')).toEqual(true);
      expect(lib.workspace.boxExists('pr2')).toEqual(true);
    });

  fw.transitionTest(
    'delete undone',
    'delete redone',
    function() {
      redo.click();
    },
    function() {
      expect(undo.getAttribute('disabled')).toBeFalsy();
      expect(redo.getAttribute('disabled')).toBeTruthy();
      expect(lib.workspace.boxExists('ex0')).toEqual(false);
      expect(lib.workspace.boxExists('pr1')).toEqual(false);
      expect(lib.workspace.boxExists('pr2')).toEqual(true);
    });
};
