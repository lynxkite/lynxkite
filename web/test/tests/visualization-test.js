'use strict';

const fs = require('fs');
const lib = require('../test-lib.js');

module.exports = function(fw) {
  // Moves all positions horizontally so that the x coordinate of the leftmost
  // position becomes zero. (Inputs and outputs string lists.)
  // Shifts in the x coordinate can happen when a second visualization is added, or due to
  // scrollbars.
  function normalized(positions) {
    let i, minx;
    for (i = 1; i < positions.length; ++i) {
      const p = positions[i];
      minx = (minx === undefined || p.x < minx) ? p.x : minx;
    }
    const result = [];
    for (i = 0; i < positions.length; ++i) {
      result.push({ x: positions[i].x - minx, y: positions[i].y });
    }
    return result;
  }

  function positions(graph) {
    const pos = [];
    for (let i = 0; i < graph.vertices.length; ++i) {
      pos.push(graph.vertices[i].pos);
    }
    return normalized(pos);
  }

  // Compare the coordinates with given precision. The compared coordinates
  // have to match on `precision` digits. For default we use 8 digits.
  function checkGraphPositions(saved, graph, precision) {
    precision = precision || 8;
    for (let i = 0; i < saved.length; ++i) {
      expect(saved[i].x).toBeCloseTo(graph[i].x, precision);
      expect(saved[i].y).toBeCloseTo(graph[i].y, precision);
    }
  }
  const visualization = lib.workspace.getStateView('vz0', 'visualization').visualization;

  const editor = lib.workspace.getVisualizationEditor('vz0');
  const name = editor.left.vertexAttribute('name');
  const gender = editor.left.vertexAttribute('gender');
  const income = editor.left.vertexAttribute('income');
  const age = editor.left.vertexAttribute('age');
  const location = editor.left.vertexAttribute('location');
  const weight = editor.left.edgeAttribute('weight');
  const comment = editor.left.edgeAttribute('comment');

  fw.transitionTest(
    'test-example workspace with example graph',
    'test-example workspace with visualization open',
    function() {
      lib.workspace.addBox({
        id: 'sg0',
        name: 'Use base graph as segmentation',
        x: 100, y: 200,
        after: 'eg0',
        params: { name: 'seg' },
      });
      lib.workspace.addBox({
        id: 'vz0',
        name: 'Graph visualization',
        x: 100, y: 300,
        after: 'sg0',
      });
      lib.workspace
        .openBoxEditor('vz0')
        .moveTo(800, 90);
      lib.workspace
        .openStateView('vz0', 'visualization')
        .moveTo(270, 90);
    },
    function() {
    });

  fw.transitionTest(
    'test-example workspace with visualization open',
    'sampled mode attribute visualizations',
    function() {
      lib.addConcurMatcher();
      //editor.left.toggleSampledVisualization();

      const expectedEdges = [
        { src: 0, dst: 1 },
        { src: 1, dst: 0 },
        { src: 2, dst: 0 },
        { src: 2, dst: 1 },
      ];
      let savedPositions;
      const DEFAULT = 'rgb(57, 188, 243)'; // Brand color.
      const LOW = 'rgb(68, 1, 84)';
      const HIGH = 'rgb(254, 232, 37)';
      // No attributes visualized.
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.edges).toConcur([
          { color: '', label: '', width: '>2' },
          { color: '', label: '', width: '>2' },
          { color: '', label: '', width: '>2' },
          { color: '', label: '', width: '>2' },
        ]);
        expect(graph.vertices).toConcur([
          { color: DEFAULT, icon: 'circle', label: '' },
          { color: DEFAULT, icon: 'circle', label: '' },
          { color: DEFAULT, icon: 'circle', label: '' },
        ]);
        savedPositions = positions(graph);
      });

      name.visualizeAs('label');
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { label: 'Adam' },
          { label: 'Eve' },
          { label: 'Bob' },
        ]);
        checkGraphPositions(positions(graph), savedPositions);
      });
      gender.visualizeAs('icon');
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { icon: 'male' },
          { icon: 'female' },
          { icon: 'male' },
        ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      income.visualizeAs('color');
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { color: LOW },
          { color: DEFAULT },
          { color: HIGH },
        ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      age.visualizeAs('size');
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { size: '<25' },
          { size: '<25' },
          { size: '>25' },
        ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      age.visualizeAs('opacity');
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { opacity: '<0.5' },
          { opacity: '<0.5' },
          { opacity: '1' },
        ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      age.visualizeAs('label-size');
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { labelSize: '<15' },
          { labelSize: '<15' },
          { labelSize: '>15' },
        ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      age.visualizeAs('label-color');
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { labelColor: 'rgb(70, 21, 102)' },
          { labelColor: LOW },
          { labelColor: HIGH },
        ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      // There is no URL attribute in the example graph. Since we only check the "href"
      // attribute anyway, any string is good enough for the test.
      name.visualizeAs('image');
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { image: 'Adam' },
          { image: 'Eve' },
          { image: 'Bob' },
        ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      // Try removing some visualizations.
      age.doNotVisualizeAs('opacity');
      age.doNotVisualizeAs('label-size');
      age.doNotVisualizeAs('label-color');
      name.doNotVisualizeAs('image');
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { opacity: '1', labelSize: '15', labelColor: 'white', image: null },
          { opacity: '1', labelSize: '15', labelColor: 'white', image: null },
          { opacity: '1', labelSize: '15', labelColor: 'white', image: null },
        ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      // Edge attributes.
      weight.visualizeAs('width');
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.edges).toConcur([
          { width: '<10' },
          { width: '<10' },
          { width: '>10' },
          { width: '>10' },
        ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      weight.visualizeAs('edge-color');
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.edges).toConcur([
          { color: LOW },
          { color: 'rgb(54, 93, 141)' },
          { color: 'rgb(57, 173, 122)' },
          { color: HIGH },
        ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      comment.visualizeAs('edge-label');
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.edges).toConcur([
          { label: 'Adam loves Eve' },
          { label: 'Eve loves Adam' },
          { label: 'Bob envies Adam' },
          { label: 'Bob loves Eve' },
        ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      // Location attributes.
      location.visualizeAs('position');
      // Toggle off and on to shake off the unpredictable offset from the non-positioned layout.
      editor.left.toggleSampledVisualization();
      editor.left.toggleSampledVisualization();
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { pos: { x: '>250', y: '>300' } },
          { pos: { x: '>250', y: '<300' } },
          { pos: { x: '<250', y: '<100' } },
        ]);
      });

      location.visualizeAs('geo-coordinates');
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { pos: { x: '<100', y: '<200' } },
          { pos: { x: '>100', y: '<200' } },
          { pos: { x: '>400', y: '>200' } },
        ]);
      });

    },
    function() {});

  fw.transitionTest(
    'test-example workspace with visualization open',
    'visualize as slider',
    function() {
      lib.addConcurMatcher();
      name.visualizeAs('label');
      age.visualizeAs('slider');
      const BLUE = 'rgb(57, 188, 243)';
      const ORANGE = 'rgb(255, 136, 0)';
      visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Adam', color: ORANGE },
          { label: 'Eve', color: ORANGE },
          { label: 'Bob', color: BLUE },
        ]);
      });
      const slider = visualization.popup.$('.slider');
      const K = protractor.Key;

      slider.sendKeys(K.HOME);
      visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Adam', color: BLUE },
          { label: 'Eve', color: BLUE },
          { label: 'Bob', color: BLUE },
        ]);
      });

      slider.sendKeys(K.RIGHT);
      visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Adam', color: BLUE },
          { label: 'Eve', color: 'white' },
          { label: 'Bob', color: BLUE },
        ]);
      });

      slider.sendKeys(K.RIGHT);
      visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Adam', color: BLUE },
          { label: 'Eve', color: ORANGE },
          { label: 'Bob', color: BLUE },
        ]);
      });

      slider.sendKeys(K.END);
      visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Adam', color: ORANGE },
          { label: 'Eve', color: ORANGE },
          { label: 'Bob', color: ORANGE },
        ]);
      });

      slider.sendKeys(K.LEFT);
      visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Adam', color: ORANGE },
          { label: 'Eve', color: ORANGE },
          { label: 'Bob', color: 'white' },
        ]);
      });

      slider.sendKeys(K.LEFT);
      visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Adam', color: ORANGE },
          { label: 'Eve', color: ORANGE },
          { label: 'Bob', color: BLUE },
        ]);
      });
    },
    function() {});

  fw.transitionTest(
    'test-example workspace with visualization open',
    'bucketed mode attribute visualizations',
    function() {
      lib.addConcurMatcher();
      editor.left.toggleBucketedVisualization();
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur([{ src: 0, dst: 0 }]);
        expect(graph.vertices).toConcur([{ label: '4' }]);
      });

      gender.visualizeAs('x');
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur([
          { src: 0, dst: 1, width: '<10' },
          { src: 1, dst: 0, width: '>10' },
          { src: 1, dst: 1, width: '<10' },
        ]);
        expect(graph.vertices).toConcur([
          { label: '1' },
          { label: '3' },
        ]);
      });

      age.visualizeAs('y');
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur([
          { src: 0, dst: 2, width: '>2' },
          { src: 2, dst: 0, width: '>2' },
          { src: 3, dst: 0, width: '>2' },
          { src: 3, dst: 2, width: '>2' },
        ]);
        expect(graph.vertices).toConcur([
          { label: '1' },
          { label: '1' },
          { label: '1' },
          { label: '1' },
        ]);
      });
    },
    function() {});

  fw.transitionTest(
    'test-example workspace with visualization open',
    'visualization for two open projects',
    function() {
      lib.addConcurMatcher();
      name.visualizeAs('label');
      let leftPositions;
      visualization.graphData().then(function(graph) {
        leftPositions = positions(graph);
        expect(graph.vertices.length).toBe(3);
      });

      editor.left.openSegmentation('seg');
      editor.right.toggleBucketedVisualization();
      editor.right.vertexAttribute('gender').visualizeAs('y');
      visualization.graphData().then(function(graph) {
        // Make sure the original vertices did not move.
        function matchPos(a, b) {
          return a.x.toFixed(3) === b.x.toFixed(3) && a.y.toFixed(3) === b.y.toFixed(3);
        }
        const pos = positions(graph);
        for (let i = 0; i < leftPositions.length; ++i) {
          let found = false;
          for (let j = 0; j < pos.length; ++j) {
            if (matchPos(pos[j], leftPositions[i])) {
              found = true;
              break;
            }
          }
          expect(found).toBe(true);
        }
        expect(graph.edges).toConcur([
          { src: 0, dst: 1, width: '<10' },
          { src: 0, dst: 4, width: '<10' },
          { src: 1, dst: 0, width: '<10' },
          { src: 1, dst: 3, width: '<10' },
          { src: 2, dst: 0, width: '<10' },
          { src: 2, dst: 1, width: '<10' },
          { src: 2, dst: 4, width: '<10' },
          { src: 3, dst: 4, width: '<10' },
          { src: 4, dst: 3, width: '>10' },
          { src: 4, dst: 4, width: '<10' },
        ]);
        expect(graph.vertices).toConcur([
          { label: 'Adam' },
          { label: 'Eve' },
          { label: 'Bob' },
          { label: '1' },
          { label: '3' },
        ]);
      });
      // Check TSV of this complex visualization.
      const expectedTSV = fs.readFileSync(__dirname + '/data/visualization-tsv-data.txt', 'utf8');
      expect(visualization.asTSV()).toEqual(expectedTSV);
    },
    function() {});

  fw.transitionTest(
    'test-example workspace with visualization open',
    'visualization context menu',
    function() {
      lib.addConcurMatcher();
      name.visualizeAs('label');
      visualization.elementByLabel('Eve').click();
      visualization.clickMenu('add-to-centers');
      visualization.popup.$('.apply-visualization-changes').click();
      editor.left.setSampleRadius(0);
      visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Adam' },
          { label: 'Eve' },
        ]);
        expect(graph.edges).toConcur([
          { src: 0, dst: 1 },
          { src: 1, dst: 0 },
        ]);
      });

      visualization.elementByLabel('Adam').click();
      visualization.clickMenu('remove-from-centers');
      visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Eve' },
        ]);
        expect(graph.edges).toEqual([]);
      });
    }, function() {});
};
