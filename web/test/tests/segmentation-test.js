'use strict';

module.exports = function() {};


var lib = require('../test-lib.js');

module.exports = function(fw) {
  fw.transitionTest(
    'test-example workspace with example graph',
    'segmentation by double created',
    function() {
      lib.workspace.addBox({
        id: 'segment-op', name: 'Segment by Double attribute', x: 100, y: 200,
        after: 'eg0', params: { attr: 'income', interval_size: '10'} });
    },
    function() {
    });

  fw.statePreservingTest(
    'segmentation by double created',
    'segmentation opens',
    function() {
      var state = lib.workspace.openStateView('segment-op', 'project');
      state.left.openSegmentation('bucketing');
      expect(state.right.segmentCount()).toEqual(2);
      state.close();
    });

  fw.transitionTest(
    'segmentation by double created',
    'segmentation copied to sub-segmentation',
    function() {
      let params = {
        apply_to_project: '|bucketing',
        name: 'copy'
      };
      lib.workspace.addBox({
        id: 'copy-op', name: 'Use base project as segmentation', x: 100, y: 300,
        after: 'segment-op', params: params });
    },
    function() {});

  fw.statePreservingTest(
    'segmentation copied to sub-segmentation',
    'sub-segmentation can be opened',
    function() {
      var state = lib.workspace.openStateView('copy-op', 'project');
      state.left.openSegmentation('bucketing');
      state.right.openSegmentation('copy');

      expect(state.left.segmentCount()).toEqual(2);
      expect(state.right.segmentCount()).toEqual(2);
      state.left.expectCurrentProjectIs('Base » bucketing');
      state.right.expectCurrentProjectIs('Base » bucketing » copy');
      // Close sub-segmentation on the right-hand side:
      state.right.close();
      // This should reopen its grandparent it's grandparent on the left:
      state.left.expectCurrentProjectIs('Base');
      state.right.expectCurrentProjectIs('Base » bucketing');

      state.close();
    });

  fw.transitionTest(
    'segmentation copied to sub-segmentation',
    'discard segmentation works',
    function() {
      lib.workspace.addBox({
        id: 'discard-segment', name: 'Discard segmentation', x: 100, y: 400,
        after: 'copy-op', params: { name: 'bucketing' } });
    },
    function() {
      var state = lib.workspace.openStateView('discard-segment', 'project');
      expect(state.left.segmentation('bucketing').isPresent()).toBe(false);
      state.close();
    });

  fw.transitionTest(
    'test-example workspace with example graph',
    'segmentation size reporting - non empty segments',
    function() {
      lib.workspace.addBox({
        id: 'copy', name: 'Use base project as segmentation', x: 100, y: 200,
        after: 'eg0', params: { name: 'self' } });
    },
    function() {
      var state = lib.workspace.openStateView('copy', 'project');
      state.left.openSegmentation('self');
      expect(state.right.getValue('segment-count')).toBe(4);
      expect(state.right.getValue('total-segment-size')).toBe(4);
      expect(state.right.getValue('total-segment-coverage')).toBe(4);
      state.close();
    });

  fw.transitionTest(
    'segmentation size reporting - non empty segments',
    'segmentation size reporting - has empty segments',
    function() {
      lib.workspace.addBox({
        id: 'filter-op', name: 'Filter by attributes', x: 100, y: 300,
        after: 'copy', params: { 'filterva_income': '*' } });
    },
    function() {
      var state = lib.workspace.openStateView('filter-op', 'project');
      state.left.openSegmentation('self');
      expect(state.right.getValue('segment-count')).toBe(4);
      expect(state.right.getValue('total-segment-size')).toBe(2);
      expect(state.right.getValue('total-segment-coverage')).toBe(2);
      expect(state.right.getValue('non-empty-segment-count')).toBe(2);
      state.close();
    });
};

