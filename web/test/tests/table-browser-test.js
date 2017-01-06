'use strict';

var lib = require('../test-lib.js');
var tableBrowser = lib.left.tableBrowser;

module.exports = function(fw) {
  fw.statePreservingTest(
    'test-example project with example graph',
    'SQL table browser open/close',
    function() {
      // Open table browser.
      tableBrowser.toggle();
      // Check tables.
      tableBrowser.expectTable(0, 'edge_attributes');
      tableBrowser.expectTableDragText(0, '`edge_attributes`');
      tableBrowser.expectTable(1, 'edges');
      tableBrowser.expectTableDragText(1, '`edges`');
      tableBrowser.expectTable(2, 'vertices');
      tableBrowser.expectTableDragText(2, '`vertices`');
      // Close table browser.
      tableBrowser.toggle();
      //lib.expectElement(lib.left.side.$('#table-browser'));
      lib.expectNotElement(lib.left.side.$('#table-browser'));
    });

  fw.statePreservingTest(
    'test-example project with example graph',
    'SQL table browser search',
    function() {
      // Open table browser.
      tableBrowser.toggle();
      tableBrowser.searchTable('edges');
      // Check tables.
      // contigous match comes first:
      tableBrowser.expectTable(0, 'edges');
      // fragmented match 'edge...s' comes after:
      tableBrowser.expectTable(1, 'edge_attributes');
      // Close table browser.
      tableBrowser.toggle();
    });

  fw.statePreservingTest(
    'test-example project with example graph',
    'SQL table browser - open and close column list',
    function() {
      tableBrowser.toggle();
      tableBrowser.toggleTable(0);
      tableBrowser.expectColumn(0, 0, '*ALL*');
      tableBrowser.expectColumnDragText(
          0, 0,
          '`comment`,\n`weight`');
      tableBrowser.expectColumn(0, 1, 'comment');
      tableBrowser.expectColumnDragText(0, 1, '`comment`');
      tableBrowser.expectColumn(0, 2, 'weight');
      tableBrowser.expectColumnDragText(0, 2, '`weight`');
      tableBrowser.toggle();
    });

  fw.statePreservingTest(
    'test-example project with example graph',
    'SQL table browser - fully qualified column names',
    function() {
      tableBrowser.toggle();
      tableBrowser.toggleTable(0);
      tableBrowser.toggleFullyQualify();
      tableBrowser.expectColumnDragText(
          0, 0,
          '`edge_attributes`.`comment`,\n`edge_attributes`.`weight`');
      tableBrowser.expectColumnDragText(
          0, 1,
          '`edge_attributes`.`comment`');
      tableBrowser.expectColumnDragText(
          0, 2,
          '`edge_attributes`.`weight`');
      tableBrowser.toggle();
    });
};
