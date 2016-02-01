'use strict';

/* global element, by */
var lib = require('../test-lib.js');
var left = lib.left;

module.exports = function(fw) {
  fw.statePreservingTest(
    'test-example project with example graph',
    'SQL default query works',
    function() {
      left.toggleSqlBox();
      left.runSql();
    
      left.expectSqlResult(
        ['age', 'gender', 'id', 'income', 'location', 'name'],
        [
          ['20.3', 'Male', '0', '1000.0', '[40.71448,-74.00598]', 'Adam'],
          ['18.2', 'Female', '1', 'null', '[47.5269674,19.0323968]', 'Eve'],
          ['50.3', 'Male', '2', '2000.0', '[1.352083,103.819836]', 'Bob'],
          ['2.0', 'Male', '3', 'null', '[-33.8674869,151.2069902]', 'Isolated Joe'],
        ]);
      // Reset state.
      left.toggleSqlBox();
    });

  fw.statePreservingTest(
    'test-example project with example graph',
    'SQL works for edge attributes',
    function() {
      left.toggleSqlBox();

      left.runSql('select edge_comment, src_name from triplets order by edge_comment');

      left.expectSqlResult(
        ['edge_comment', 'src_name'],
        [
          [ 'Adam loves Eve', 'Adam' ],
          [ 'Bob envies Adam', 'Bob' ],
          [ 'Bob loves Eve', 'Bob' ],
          [ 'Eve loves Adam', 'Eve' ],
        ]);

      // Reset state.
      left.toggleSqlBox();
    });

  fw.statePreservingTest(
    'test-example project with example graph',
    'Save SQL result as CSV works',
    function() {
      left.toggleSqlBox();

      left.setSql('select name, age, income from vertices order by name');

      left.startSqlSaving();

      // Choose csv format.
      left.side.element(by.css('#exportFormat option[value="csv"]')).click();

      // And go.
      lib.startDownloadWatch();
      left.executeSqlSaving();
      var downloadedFileName = lib.waitForNewDownload(/\.csv$/);
      lib.expectFileContents(
        downloadedFileName,
        'Adam,20.3,1000.0\nBob,50.3,2000.0\nEve,18.2,\nIsolated Joe,2.0,\n');

      // Reset state.
      left.toggleSqlBox();
    });
};
