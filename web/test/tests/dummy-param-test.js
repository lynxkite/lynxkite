'use strict';

module.exports = function() {};

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test-example workspace with example graph',
    'rename attributes',
    function() {
      lib.workspace.addBox({
        id: 'rename-vertex-attrs',
        name: 'Rename vertex attributes',
        x: 100,
        y: 200,
        after: 'eg0',
        params: {change_age: "new_age"}
      });
      lib.workspace.clickBox('rename-vertex-attrs');
    }, function() {
      lib.expectElement($('#text-title'));
      lib.expectElement($('#text #title2'));
    }, 1
  );
};
