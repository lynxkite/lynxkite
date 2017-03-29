'use strict';

module.exports = function() {};


var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test-example workspace with example graph',
    'segmentation by double created',
    function() {
      var exampleGraphOp = lib.workspace.getBox(0);
      var params = {
        'attr': 'income',
        'interval_size': '10',
      };
      lib.workspace.addBox('Segment by double attribute', 100, 200);
      var segmentOp = lib.workspace.getBox(1);
      lib.workspace.connectBoxes(
          exampleGraphOp, 'project',
          segmentOp, 'project');
      lib.workspace.editBox(segmentOp, params);
    },
    function() {
    });

  fw.transitionTest(
    'segmentation by double created',
    'segmentation opens',
    function() {
      lib.workspace.selectOutput(lib.workspace.getBox(1), 'project');
      lib.left.openSegmentation('bucketing');
    },
    function() {
      expect(lib.right.segmentCount()).toEqual(2);
    });

  fw.transitionTest(
    'segmentation opens',
    'sub-segmentation can be created and opened',
    function() {
      var segmentOp1 = lib.workspace.getBox(1);
      lib.workspace.addBox('Copy graph into a segmentation', 100, 300);
      var segmentOp2 = lib.workspace.getBox(2);
      lib.workspace.connectBoxes(
          segmentOp1, 'project',
          segmentOp2, 'project');
      lib.workspace.editBox(segmentOp2, {
          'apply_to_project': '|bucketing',
          'name': 'copy',
      });
      lib.workspace.selectOutput(segmentOp2, 'project');
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
      var segmentOp2 = lib.workspace.getBox(2);
      lib.workspace.addBox('Discard segmentation', 100, 400);
      var discardOp = lib.workspace.getBox(3);
      lib.workspace.connectBoxes(
          segmentOp2, 'project',
          discardOp, 'project');
      lib.workspace.editBox(discardOp, {
          'name': 'bucketing',
      });
      lib.workspace.selectOutput(discardOp, 'project');
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
      var exampleGraphOp = lib.workspace.getBox(0);
      lib.workspace.addBox('Copy graph into a segmentation', 100, 200);
      var segmentOp = lib.workspace.getBox(1);
      lib.workspace.connectBoxes(
          exampleGraphOp, 'project',
          segmentOp, 'project');
      lib.workspace.editBox(segmentOp, params);
      lib.workspace.selectOutput(segmentOp, 'project');
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
      var segmentOp = lib.workspace.getBox(1);
      lib.workspace.addBox('Filter by attributes', 100, 300);
      var filterOp = lib.workspace.getBox(2);
      lib.workspace.connectBoxes(
          segmentOp, 'project',
          filterOp, 'project');
      lib.workspace.editBox(filterOp, params);
      lib.workspace.selectOutput(filterOp, 'project');
      lib.left.openSegmentation('self');
    },
    function() {
      expect(lib.right.getValue('segment-count')).toBe(4);
      expect(lib.right.getValue('total-segment-size')).toBe(2);
      expect(lib.right.getValue('total-segment-coverage')).toBe(2);
      expect(lib.right.getValue('non-empty-segment-count')).toBe(2);
    });
};

