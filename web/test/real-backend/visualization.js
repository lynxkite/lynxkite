'use strict';

var fs = require('fs');
var lib = require('./test-lib.js');

module.exports = function(fw) {
  // A matcher for lists of objects that ignores fields not present in the reference.
  function addConcurMatcher() {
    jasmine.addMatchers({
      toConcur: function(util, customEqualityTesters) {
        return { compare: function(actual, expected) {
          if (actual.length !== expected.length) {
            return { pass: false };
          }
          for (var i = 0; i < actual.length; ++i) {
            var keys = Object.keys(expected[i]);
            for (var j = 0; j < keys.length; ++j) {
              var av = actual[i][keys[j]];
              var ev = expected[i][keys[j]];
              if (!util.equals(av, ev, customEqualityTesters)) {
                return { pass: false };
              }
            }
          }
          return { pass: true };
        }};
      }});
  }

  function positions(graph) {
    var pos = [];
    for (var i = 0; i < graph.vertices.length; ++i) {
      pos.push(graph.vertices[i].pos);
    }
    return pos;
  }

  // Moves all positions horizontally so that the x coordinate of the leftmost
  // position becomes zero. (Inputs and outputs string lists.)
  function normalize(positions) {
    var i, minx;
    var result = [];
    for (i = 1; i < positions.length; ++i) {
      var parts = positions[i].split(' ');
      var p = { x: parseFloat(parts[0]), y: parseFloat(parts[1]) };
      result.push(p);
      minx = minx === undefined || p.x < minx ? p.x : minx;
    }
    for (i = 0; i < result.length; ++i) {
      result[i].x -= minx;
      result[i] = result[i].x.toFixed(3) + ' ' + result[i].y.toFixed(3);
    }
    return result;
  }

  fw.statePreservingTest(
    'test-example project with example graph',
    'sampled mode attribute visualizations',
    function() {
      addConcurMatcher();
      lib.left.toggleSampledVisualization();

      var expectedEdges = [
        { src : 0, dst: 1 },
        { src : 1, dst: 0 },
        { src : 2, dst: 0 },
        { src : 2, dst: 1 },
      ];
      var savedPositions;

      // No attributes visualized.
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.edges).toConcur([
          { color: '', label: '', width: '4.048' },
          { color: '', label: '', width: '4.048' },
          { color: '', label: '', width: '4.048' },
          { color: '', label: '', width: '4.048' },
          ]);
        expect(graph.vertices).toConcur([
          { color: 'rgb(107, 107, 107)', icon: 'circle', label: '' },
          { color: 'rgb(107, 107, 107)', icon: 'circle', label: '' },
          { color: 'rgb(107, 107, 107)', icon: 'circle', label: '' },
          ]);
        savedPositions = positions(graph);
      });

      lib.left.visualizeAttribute('name', 'label');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { label: 'Adam' },
          { label: 'Eve' },
          { label: 'Bob' },
          ]);
        expect(positions(graph)).toEqual(savedPositions);
      });

      lib.left.visualizeAttribute('gender', 'icon');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { icon: 'male' },
          { icon: 'female' },
          { icon: 'male' },
          ]);
        expect(positions(graph)).toEqual(savedPositions);
      });

      lib.left.visualizeAttribute('income', 'color');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { color: 'rgb(53, 53, 161)' },
          { color: 'rgb(107, 107, 107)' },
          { color: 'rgb(161, 53, 53)' },
          ]);
        expect(positions(graph)).toEqual(savedPositions);
      });

      lib.left.visualizeAttribute('age', 'size');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { size: '12.858032957292307' },
          { size: '12.17481048468403' },
          { size: '20.240000000000002' },
          ]);
        expect(positions(graph)).toEqual(savedPositions);
      });

      lib.left.visualizeAttribute('age', 'opacity');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { opacity: '0.4035785288270378' },
          { opacity: '0.36182902584493043' },
          { opacity: '1' },
          ]);
        expect(positions(graph)).toEqual(savedPositions);
      });

      lib.left.visualizeAttribute('age', 'label-size');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { labelSize: '12.107355864811135px' },
          { labelSize: '10.854870775347912px' },
          { labelSize: '30px' },
          ]);
        expect(positions(graph)).toEqual(savedPositions);
      });

      lib.left.visualizeAttribute('age', 'label-color');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { labelColor: 'rgb(66, 53, 161)' },
          { labelColor: 'rgb(53, 53, 161)' },
          { labelColor: 'rgb(161, 53, 53)' },
          ]);
        expect(positions(graph)).toEqual(savedPositions);
      });

      // We don't have a URL attribute. Since we only look at the "href" anyway, anything will do.
      lib.left.visualizeAttribute('name', 'image');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { image: 'Adam' },
          { image: 'Eve' },
          { image: 'Bob' },
          ]);
        expect(positions(graph)).toEqual(savedPositions);
      });

      // Try removing some visualizations.
      lib.left.doNotVisualizeAttribute('age', 'opacity');
      lib.left.doNotVisualizeAttribute('age', 'label-size');
      lib.left.doNotVisualizeAttribute('age', 'label-color');
      lib.left.doNotVisualizeAttribute('name', 'image');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { opacity: '1', labelSize: '15px', labelColor: '', image: null },
          { opacity: '1', labelSize: '15px', labelColor: '', image: null },
          { opacity: '1', labelSize: '15px', labelColor: '', image: null },
          ]);
        expect(positions(graph)).toEqual(savedPositions);
      });

      // Edge attributes.
      lib.left.visualizeAttribute('weight', 'width');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.edges).toConcur([
          { width: '2.5300000000000002' },
          { width: '5.0600000000000005' },
          { width: '7.590000000000002' },
          { width: '10.120000000000001' },
        ]);
        expect(positions(graph)).toEqual(savedPositions);
      });

      lib.left.visualizeAttribute('weight', 'edge-color');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.edges).toConcur([
          { color: 'rgb(53, 53, 161)' },
          { color: 'rgb(125, 53, 161)' },
          { color: 'rgb(161, 53, 125)' },
          { color: 'rgb(161, 53, 53)' },
        ]);
        expect(positions(graph)).toEqual(savedPositions);
      });

      lib.left.visualizeAttribute('comment', 'edge-label');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.edges).toConcur([
          { label: 'Adam loves Eve' },
          { label: 'Eve loves Adam' },
          { label: 'Bob envies Adam' },
          { label: 'Bob loves Eve' },
        ]);
        expect(positions(graph)).toEqual(savedPositions);
      });

      // Location attributes.
      lib.left.visualizeAttribute('location', 'position');
      // Toggle off and on to shake off the unpredictable offset from the non-positioned layout.
      lib.left.toggleSampledVisualization();
      lib.left.toggleSampledVisualization();
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { pos: '563.5240305688799 227.70000000000005' },
          { pos: '571.2779522478335 121.80442258665076' },
          { pos: '518.7220477521668 25.299999999999997' },
          ]);
      });

      lib.left.visualizeAttribute('location', 'geo');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { pos: '400.192773769436 72.88458991024139' },
          { pos: '547.2699646213099 57.85735348192763' },
          { pos: '681.3038848805471 141.32935071053427' },
          ]);
      });

      lib.openProject('test-example'); // Restore state.
    });

  fw.statePreservingTest(
    'test-example project with example graph',
    'visualize as slider',
    function() {
      addConcurMatcher();
      lib.left.toggleSampledVisualization();
      lib.left.visualizeAttribute('name', 'label');
      lib.left.visualizeAttribute('age', 'slider');
      var RED = 'rgb(161, 53, 53)',
          YELLOW = 'rgb(184, 184, 46)',
          GREEN = 'rgb(53, 161, 53)';
      lib.visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Adam', color: GREEN },
          { label: 'Eve', color: GREEN },
          { label: 'Bob', color: RED },
          ]);
      });
      var slider = lib.left.attributeSlider('age');
      /* global protractor */
      var K = protractor.Key;

      slider.sendKeys(K.HOME);
      lib.visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Adam', color: RED },
          { label: 'Eve', color: RED },
          { label: 'Bob', color: RED },
          ]);
      });

      slider.sendKeys(K.RIGHT);
      lib.visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Adam', color: RED },
          { label: 'Eve', color: YELLOW },
          { label: 'Bob', color: RED },
          ]);
      });

      slider.sendKeys(K.RIGHT);
      lib.visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Adam', color: RED },
          { label: 'Eve', color: GREEN },
          { label: 'Bob', color: RED },
          ]);
      });

      slider.sendKeys(K.END);
      lib.visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Adam', color: GREEN },
          { label: 'Eve', color: GREEN },
          { label: 'Bob', color: GREEN },
          ]);
      });

      slider.sendKeys(K.LEFT);
      lib.visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Adam', color: GREEN },
          { label: 'Eve', color: GREEN },
          { label: 'Bob', color: YELLOW },
          ]);
      });

      slider.sendKeys(K.LEFT);
      lib.visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Adam', color: GREEN },
          { label: 'Eve', color: GREEN },
          { label: 'Bob', color: RED },
          ]);
      });
    });

  fw.statePreservingTest(
    'test-example project with example graph',
    'bucketed mode attribute visualizations',
    function() {
      addConcurMatcher();
      lib.left.toggleBucketedVisualization();
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur([{ src: 0, dst: 0 }]);
        expect(graph.vertices).toConcur([{ label: '4' }]);
      });

      lib.left.visualizeAttribute('gender', 'x');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur([
          { src: 0, dst: 1, width: '4.048' },
          { src: 1, dst: 0, width: '8.096' },
          { src: 1, dst: 1, width: '4.048' },
          ]);
        expect(graph.vertices).toConcur([
          { label: '1' },
          { label: '3' },
          ]);
      });

      lib.left.visualizeAttribute('age', 'y');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur([
          { src: 0, dst: 2, width: '4.048' },
          { src: 2, dst: 0, width: '4.048' },
          { src: 3, dst: 0, width: '4.048' },
          { src: 3, dst: 2, width: '4.048' },
          ]);
        expect(graph.vertices).toConcur([
          { label: '1' },
          { label: '1' },
          { label: '1' },
          { label: '1' },
          ]);
      });

      lib.openProject('test-example'); // Restore state.
    });

  fw.statePreservingTest(
    'test-example project with example graph',
    'visualization for two open projects',
    function() {
      addConcurMatcher();
      lib.left.toggleSampledVisualization();
      lib.left.visualizeAttribute('name', 'label');
      var leftPositions;
      lib.visualization.graphData().then(function(graph) {
        leftPositions = normalize(positions(graph));
        expect(graph.vertices.length).toBe(3);
      });

      lib.right.openSecondProject('test-example');
      lib.right.toggleBucketedVisualization();
      lib.right.visualizeAttribute('gender', 'y');
      lib.visualization.graphData().then(function(graph) {
        // Make sure the original vertices did not move.
        var pos = normalize(positions(graph));
        for (var i = 0; i < leftPositions.length; ++i) {
          var found = false;
          for (var j = 0; j < pos.length; ++j) {
            if (pos[j] === leftPositions[i]) {
              found = true;
              break;
            }
          }
          expect(found).toBe(true);
        }
        expect(graph.edges).toConcur([
          { src: 0, dst: 1, width: '4.048' },
          { src: 0, dst: 3, width: '4.048' },
          { src: 1, dst: 0, width: '4.048' },
          { src: 1, dst: 4, width: '4.048' },
          { src: 2, dst: 0, width: '4.048' },
          { src: 2, dst: 1, width: '4.048' },
          { src: 2, dst: 3, width: '4.048' },
          { src: 2, dst: 4, width: '4.048' },
          { src: 3, dst: 0, width: '4.048' },
          { src: 3, dst: 4, width: '4.048' },
          { src: 4, dst: 0, width: '4.048' },
          { src: 4, dst: 1, width: '8.096' },
          { src: 4, dst: 3, width: '8.096' },
          { src: 4, dst: 4, width: '4.048' },
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
      var expectedTSV = fs.readFileSync('test/real-backend/visualization-tsv-data.txt', 'utf8');
      expect(lib.visualization.asTSV()).toEqual(expectedTSV);

      lib.openProject('test-example'); // Restore state.
    });

  fw.statePreservingTest(
    'test-example project with example graph',
    'visualization context menu',
    function() {
      addConcurMatcher();
      lib.left.toggleSampledVisualization();
      lib.left.visualizeAttribute('name', 'label');
      lib.visualization.vertexByLabel('Eve').click();
      lib.visualization.clickMenu('add-to-centers');
      lib.left.setSampleRadius(0);
      lib.visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Adam' },
          { label: 'Eve' },
        ]);
        expect(graph.edges).toConcur([
          { src: 0, dst: 1 },
          { src: 1, dst: 0 },
        ]);
      });

      lib.visualization.vertexByLabel('Adam').click();
      lib.visualization.clickMenu('remove-from-centers');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Eve' },
        ]);
        expect(graph.edges).toEqual([]);
      });

      lib.openProject('test-example'); // Restore state.
    });
};
