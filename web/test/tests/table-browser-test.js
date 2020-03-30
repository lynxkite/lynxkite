'use strict';

const lib = require('../test-lib.js');

module.exports = function(fw) {

  fw.transitionTest(
    'snapshot created',
    'SQL table browser - global SQL box',
    function() {
      lib.splash.newDirectory('plum');
      lib.splash.popDirectory();
      lib.splash.openGlobalSqlBox();
      const tableBrowser = lib.splash.tableBrowser;
      tableBrowser.expectNode([0], 'plum', '`plum`');
      tableBrowser.expectNode([1], 'This is a snapshot!', '`This is a snapshot!`');
      tableBrowser.toggleNode([1]);
      tableBrowser.expectNode([1, 0], 'vertices', '`This is a snapshot!.vertices`');
      tableBrowser.enterSearchQuery('snap');
      tableBrowser.expectNode([0], 'This is a snapshot!', '`This is a snapshot!`');
      tableBrowser.toggle();
    },
    function() {
    });

  fw.transitionTest(
    'snapshot created',
    'SQL table browser - global SQL box - dot in the folder name',
    function() {
      lib.splash.newDirectory('firstname.lastname@lynx.com');
      lib.splash.newDirectory('dir.v01');
      lib.splash.newDirectory('inner');
      lib.splash.popDirectory();
      lib.splash.popDirectory();
      lib.splash.openGlobalSqlBox();
      const tableBrowser = lib.splash.tableBrowser;
      tableBrowser.expectNode([0], 'dir.v01', '`dir.v01`');
      tableBrowser.toggleNode([0]);
      tableBrowser.expectNode([0, 0], 'inner', '`dir.v01/inner`');
      lib.splash.popDirectory();
    },
    function() {
    });
};
