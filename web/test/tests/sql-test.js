'use strict';

/* global element, by */
var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'test-example project with example graph',
    'SQL queries work',
    function() {
      var toggleButton = lib.left.side.element(by.id('sql-toggle'));
      var editor = lib.left.side.element(by.id('sql-editor'));
      var runButton = lib.left.side.element(by.id('run-sql-button'));
      toggleButton.click();

      // Run the default (select * from `!vertices`) query.
      runButton.click();
      expect(editor.evaluate('result.header')).toEqual(
        ['age', 'gender', 'id', 'income', 'location', 'name']);
      expect(editor.evaluate('result.data')).toEqual([
        ['20.3', 'Male', '0', '1000.0', '[40.71448,-74.00598]', 'Adam'],
        ['18.2', 'Female', '1', 'null', '[47.5269674,19.0323968]', 'Eve'],
        ['50.3', 'Male', '2', '2000.0', '[1.352083,103.819836]', 'Bob'],
        ['2.0', 'Male', '3', 'null', '[-33.8674869,151.2069902]', 'Isolated Joe'],
      ]);

      // Query some edge attributes.
      lib.sendKeysToACE(editor, 'select comment, `src$name` from `!edges` order by comment');
      runButton.click();
      expect(editor.evaluate('result.header')).toEqual(['comment', 'src$name']);
      expect(editor.evaluate('result.data')).toEqual([
        [ 'Adam loves Eve', 'Adam' ],
        [ 'Bob envies Adam', 'Bob' ],
        [ 'Bob loves Eve', 'Bob' ],
        [ 'Eve loves Adam', 'Eve' ],
      ]);

      // Restore the state.
      toggleButton.click();
    });
};
