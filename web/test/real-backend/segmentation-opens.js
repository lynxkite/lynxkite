'use strict';

var lib = require('./test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test-example project with example graph',
    'segmentation by double created',
    function() {
      var params = {
        'attr': 'income',
        'interval-size': '10',
      };
      lib.runLeftOperation('Segment by double attribute', params);
    },
    function() {
    });
  fw.transitionTest(
    'segmentation by double created',
    'segmentation opens',
    function() {
      lib.openSegmentation('bucketing');      
    },
    function() {
      expect(lib.segmentCount()).toEqual(2);
    });
};
