'use strict';

var lib = require('./test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'empty test-example project',
    'test-example project with history',
    function() {
      lib.runLeftOperation('example graph');
      lib.runLeftOperation(
          'degree',
           {
            'name': 'deg',
            'direction': 'incoming edges',
          });
      lib.runLeftOperation(
          'add constant vertex attribute',
          {
            'name': 'c',
            'value': '300000',
          });
      lib.runLeftOperation(
          'derived edge attribute',
          {
            'output': 'foo',
            'expr': 'src$deg + dst$deg',
          });
      lib.openLeftProjectHistory();
    },
    function() {
    });
};
