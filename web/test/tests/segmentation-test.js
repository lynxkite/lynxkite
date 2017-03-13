'use strict';

module.exports = function() {};

/*
var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test-example project with example graph',
    'segmentation by double created',
    function() {
      var params = {
        'attr': 'income',
        'interval_size': '10',
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
    },
    function() {
      lib.right.expectCurrentProjectIs('test-example|bucketing|copy');
    });

  fw.transitionTest(
    'sub-segmentation can be created and opened',
    'closing sub-segmentation on the RHS reopens its grandparent',
    function() {
      lib.right.close();
    },
    function() {
      lib.left.expectCurrentProjectIs('test-example');
      lib.right.expectCurrentProjectIs('test-example|bucketing');
    });

  fw.transitionTest(
    'closing sub-segmentation on the RHS reopens its grandparent',
    'discard segmentation works',
    function() {
      lib.left.runOperation('Discard segmentation', { name: 'bucketing' });
    },
    function() {
      lib.left.expectCurrentProjectIs('test-example');
      lib.right.expectCurrentProjectIsError();
    });

  fw.transitionTest(
    'test-example project with example graph',
    'segmentation size reporting - non empty segments',
    function() {
      var params = {
        'name': 'self',
      };
      lib.left.runOperation('Copy graph into a segmentation', params);
      lib.left.openSegmentation('self');
    },
    function() {
      expect(lib.right.getValue('segment-count')).toBe(4);
      expect(lib.right.getValue('total-segment-size')).toBe(4);
      expect(lib.right.getValue('total-segment-coverage')).toBe(4);
    });

  fw.transitionTest(
    'segmentation size reporting - non empty segments',
    'segmentation size reporting - has empty segments',
    function() {
      var params = {
        'filterva_income': '*',
      };
      lib.left.runOperation('Filter by attributes', params);
    },
    function() {
      expect(lib.right.getValue('segment-count')).toBe(4);
      expect(lib.right.getValue('total-segment-size')).toBe(2);
      expect(lib.right.getValue('total-segment-coverage')).toBe(2);
      expect(lib.right.getValue('non-empty-segment-count')).toBe(2);
    });
};
*/
