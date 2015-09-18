'use strict';

var lib = require('./test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'test-example project with example graph',
    'name histogram looks good',
    function() {
      lib.getLeftHistogramValues('name');
      console.log('fuck4');
    });
};
