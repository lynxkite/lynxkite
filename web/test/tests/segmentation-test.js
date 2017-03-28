'use strict';

module.exports = function() {};


var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test-example workspace with example graph',
    'segmentation by double created',
    function() {
      var params = {
        'attr': 'income',
        'interval_size': '10',
      };
      lib.workspace.addBox('Segment by double attribute', 100, 200, 'segment-op', [{
        boxID: 'first-example-graph',
        srcPlugID: 'project',
        dstPlugID: 'project'
      }]);
      lib.workspace.editBox('segment-op', params);
    },
    function() {
    });

  fw.transitionTest(
    'segmentation by double created',
    'segmentation opens',
    function() {
      lib.workspace.selectOutput('segment-op', 'project');
      lib.left.openSegmentation('bucketing');
    },
    function() {
      expect(lib.right.segmentCount()).toEqual(2);
    });

  fw.transitionTest(
    'segmentation opens',
    'sub-segmentation can be created and opened',
    function() {
      lib.workspace.addBox('Copy graph into a segmentation', 100, 300, 'copy-as-segment', [{
        boxID: 'segment-op',
        srcPlugID: 'project',
        dstPlugID: 'project'
      }]);
      lib.workspace.editBox('copy-as-segment', {
          'apply_to_project': '|bucketing',
          'name': 'copy',
      });
      lib.workspace.selectOutput('copy-as-segment', 'project');
      lib.left.openSegmentation('bucketing');
      lib.right.openSegmentation('copy');
    },
    function() {
      expect(lib.left.segmentCount()).toEqual(2);
      expect(lib.right.segmentCount()).toEqual(2);
      lib.left.expectCurrentProjectIs('State » bucketing');
      lib.right.expectCurrentProjectIs('State » bucketing » copy');
    });

  fw.transitionTest(
    'sub-segmentation can be created and opened',
    'closing sub-segmentation on the RHS reopens its grandparent',
    function() {
      lib.right.close();
    },
    function() {
      lib.left.expectCurrentProjectIs('State');
      lib.right.expectCurrentProjectIs('State » bucketing');
    });

  fw.transitionTest(
    'closing sub-segmentation on the RHS reopens its grandparent',
    'discard segmentation works',
    function() {
      lib.workspace.addBox('Discard segmentation', 100, 400, 'discard-segment', [{
        boxID: 'copy-as-segment',
        srcPlugID: 'project',
        dstPlugID: 'project'
      }]);
      lib.workspace.editBox('discard-segment', {
          'name': 'bucketing',
      });
      lib.workspace.selectOutput('discard-segment', 'project');
    },
    function() {
      expect(lib.left.segmentation('bucketing').isPresent()).toBe(false);
    });

  fw.transitionTest(
    'test-example workspace with example graph',
    'segmentation size reporting - non empty segments',
    function() {
      var params = {
        'name': 'self',
      };
      lib.workspace.addBox('Copy graph into a segmentation', 100, 200, 'copy-as-segment', [{
        boxID: 'first-example-graph',
        srcPlugID: 'project',
        dstPlugID: 'project'
      }]);
      lib.workspace.editBox('copy-as-segment', params);
      lib.workspace.selectOutput('copy-as-segment', 'project');
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
      lib.workspace.addBox('Filter by attributes', 100, 300, 'filter-op', [{
        boxID: 'copy-as-segment',
        srcPlugID: 'project',
        dstPlugID: 'project'
      }]);
      lib.workspace.editBox('filter-op', params);
      lib.workspace.selectOutput('filter-op', 'project');
      lib.left.openSegmentation('self');
    },
    function() {
      expect(lib.right.getValue('segment-count')).toBe(4);
      expect(lib.right.getValue('total-segment-size')).toBe(2);
      expect(lib.right.getValue('total-segment-coverage')).toBe(2);
      expect(lib.right.getValue('non-empty-segment-count')).toBe(2);
    });
};

