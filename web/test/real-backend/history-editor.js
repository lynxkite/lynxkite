'use strict';

var lib = require('./test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'empty test-example project',
    'test-example project with history',
    function() {
      lib.left.runOperation('example graph');
      lib.left.runOperation(
          'degree',
           {
            name: 'deg',
            direction: 'incoming edges',
          });
      lib.left.runOperation(
          'add constant vertex attribute',
          {
            name: 'c',
            value: '300000',
          });
      lib.left.runOperation(
          'derived edge attribute',
          {
            output: 'foo',
            expr: 'src$deg + dst$deg',
          });
      lib.left.openProjectHistory();
    },
    function() {
      expect(lib.left.getProjectHistory().isDisplayed()).toBe(true);
    });
};
