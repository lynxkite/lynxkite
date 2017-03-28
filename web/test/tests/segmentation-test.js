'use strict';

module.exports = function() {};


var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test-example workspace with example graph',
    'segmentation by double created',
    function() {
      lib.workspace.addBox({
        id: 'segment-op',
        name: 'segment by double attribute',
        x: 100,
        y: 200,
        inputs: [{
          boxID: 'first-example-graph',
          srcPlugID: 'project',
          dstPlugID: 'project'
        }],
        params: {
          'attr': 'income',
          'interval_size': '10',
        },
      });
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
      lib.workspace.addBox({
        id: 'copy-as-segment',
        name: 'copy graph into a segmentation',
        x: 100,
        y: 300,
        inputs: [{
          boxID: 'segment-op',
          srcPlugID: 'project',
          dstPlugID: 'project'
        }],
        params: {
          'apply_to_project': '|bucketing',
          'name': 'copy',
        },
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
      lib.workspace.addBox({
        id: 'discard-segment',
        name: 'discard segmentation',
        x: 100,
        y: 400,
        inputs: [{
          boxID: 'copy-as-segment',
          srcPlugID: 'project',
          dstPlugID: 'project'
        }],
        params: {
          'name': 'bucketing',
        },
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
      lib.workspace.addBox({
        id: 'copy-as-segment',
        name: 'copy graph into a segmentation',
        x: 100,
        y: 200,
        inputs: [{
          boxID: 'first-example-graph',
          srcPlugID: 'project',
          dstPlugID: 'project'
        }],
        params: {
          'name': 'self',
        },
      });
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
      lib.workspace.addBox({
        id: 'filter-op',
        name: 'filter by attributes',
        x: 100,
        y: 300,
        inputs: [{
          boxID: 'copy-as-segment',
          srcPlugID: 'project',
          dstPlugID: 'project'
        }],
        params: {
          'filterva_income': '*',
        },
      });
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

