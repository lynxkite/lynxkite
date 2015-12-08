'use strict';

var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test-example project with example graph',
    'segmentation by double created',
    function() {
      var params = {
        'attr': 'income',
        'interval-size': '10',
      };
      lib.left.runOperation('Segment by double attribute', params);
    },
    function() {
    });
  fw.transitionTest(
    'segmentation by double created',
    'segmentation opens',
    function() {
      lib.left.openSegmentation('bucketing');
    },
    function() {
      expect(lib.right.segmentCount()).toEqual(2);
    });
  fw.transitionTest(
    'segmentation opens',
    'sub-segmentation can be created and opened',
    function() {
      lib.right.runOperation('Copy graph into a segmentation', {'name': 'copy'});
      lib.right.openSegmentation('copy');
      lib.right.expectCurrentProjectIs('example|bucketing|copy');
    },
    function() {
    });
  fw.transitionTest(
    'sub-segmentation can be created and opened',
    'closing sub-segmentation on the RHS reopens its grandparent',
    function() {
      lib.right.close();
      lib.left.expectCurrentProjectIs('example');
      lib.right.expectCurrentProjectIs('example|bucketing');
    },
    function() {
    });
};
