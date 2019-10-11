'use strict';

const lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test-example workspace with example graph',
    'rename vertex attributes',
    function() {
      lib.workspace.addBox({
        id: 'rename-vertex-attrs',
        name: 'Rename vertex attributes',
        x: 100,
        y: 200,
        after: 'eg0',
        params: {change_age: 'new_age'}
      });
      lib.workspace.clickBox('rename-vertex-attrs');
    }, function() {
      lib.expectHasText($('#text-title'), 'The new names for each attribute:');
      lib.expectElement($('#text-title #help-button'));
    }
  );
  fw.transitionTest(
    'test-example workspace with example graph',
    'rename edge attributes',
    function() {
      lib.workspace.addBox({
        id: 'rename-edge-attrs',
        name: 'Rename edge attributes',
        x: 200,
        y: 200,
        after: 'eg0',
        params: {change_weight: 'new_weight'}
      });
      lib.workspace.clickBox('rename-edge-attrs');
    }, function() {
      lib.expectHasText($('#text-title'), 'The new names for each attribute:');
      lib.expectElement($('#text-title #help-button'));
    }
  );
};
