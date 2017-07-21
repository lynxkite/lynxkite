'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {

  fw.transitionTest(
    'snapshot created',
    'SQL table browser - global SQL box',
    function() {
      lib.splash.newDirectory('plum');
      lib.splash.popDirectory();
      lib.splash.openGlobalSqlBox();
      var tableBrowser = lib.splash.tableBrowser;
      tableBrowser.expectNode([0], 'plum', '`plum`');
      tableBrowser.expectNode([1], 'This is a snapshot!', '`This is a snapshot!`');
      tableBrowser.toggleNode([1]);
      tableBrowser.expectNode([1, 0], 'vertices', '`This is a snapshot!.vertices`'); // bug
      tableBrowser.enterSearchQuery('snap');
      tableBrowser.expectNode([0], 'This is a snapshot!', '`This is a snapshot!`');
      tableBrowser.toggle();
    },
    function() {
    });
};

