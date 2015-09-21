'use strict';

var lib = require('./test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'test-example project with example graph',
    'name histogram looks good',
    function() {
      expect(lib.left.getHistogramValues('name').then(lib.sortHistogramValues)).toEqual([
        { title: 'Adam', size: '100', value: '1' },
        { title: 'Bob', size: '100', value: '1' },
        { title: 'Eve', size: '100', value: '1' },
        { title: 'Isolated Joe', size: '100', value: '1' },
      ]);
    });
};
