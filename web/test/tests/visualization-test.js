'use strict';

var fs = require('fs');
var lib = require('../test-lib.js');

module.exports = function(fw) {
  // A matcher for lists of objects that ignores fields not present in the reference.
  // Example use:
  //   expect([{ a: 1, b: 1234 }, { a: 2, b: 2345 }]).toConcur([{ a: 1 }, { a: 2 }]);
  // Constraints in strings are also accepted for numerical values. E.g. '<5'.
  // Objects are recursively checked.
  function addConcurMatcher() {
    jasmine.addMatchers({
      toConcur: function(util, customEqualityTesters) {
        return { compare: function(actual, expected) {
          function match(actual, expected) {
            if (expected === null) {
              return actual === null;
            } else if (typeof expected === 'object') {
              var keys = Object.keys(expected);
              for (var i = 0; i < keys.length; ++i) {
                var av = actual[keys[i]];
                var ev = expected[keys[i]];
                if (!match(av, ev)) {
                  return false;
                }
              }
              return true;
            } else if (typeof expected === 'string' && expected[0] === '<') {
              return actual < parseFloat(expected.slice(1));
            } else if (typeof expected === 'string' && expected[0] === '>') {
              return actual > parseFloat(expected.slice(1));
            } else {
              return util.equals(actual, expected, customEqualityTesters);
            }
          }

          if (actual.length !== expected.length) {
            return { pass: false };
          }
          for (var i = 0; i < actual.length; ++i) {
            if (!match(actual[i], expected[i])) {
              return { pass: false };
            }
          }
          return { pass: true };
        }};
      }});
  }

  // Moves all positions horizontally so that the x coordinate of the leftmost
  // position becomes zero. (Inputs and outputs string lists.)
  // Shifts in the x coordinate can happen when a second visualization is added, or due to
  // scrollbars.
  function normalized(positions) {
    var i, minx;
    for (i = 1; i < positions.length; ++i) {
      var p = positions[i];
      minx = (minx === undefined || p.x < minx) ? p.x : minx;
    }
    var result = [];
    for (i = 0; i < positions.length; ++i) {
      result.push({ x: positions[i].x - minx, y: positions[i].y });
    }
    return result;
  }

  function positions(graph) {
    var pos = [];
    for (var i = 0; i < graph.vertices.length; ++i) {
      pos.push(graph.vertices[i].pos);
    }
    return normalized(pos);
  }

  // Compare the coordinates with given precision. The compared coordinates
  // have to match on `precision` digits. For default we use 8 digits.
  function checkGraphPositions(saved, graph, precision) {
    precision = precision || 8;
    for(var i = 0; i < saved.length; ++i) {
      expect(saved[i].x).toBeCloseTo(graph[i].x, precision);
      expect(saved[i].y).toBeCloseTo(graph[i].y, precision);
    }
  }

  var name = lib.left.vertexAttribute('name');
  var gender = lib.left.vertexAttribute('gender');
  var income = lib.left.vertexAttribute('income');
  var age = lib.left.vertexAttribute('age');
  var location = lib.left.vertexAttribute('location');
  var weight = lib.left.edgeAttribute('weight');
  var comment = lib.left.edgeAttribute('comment');

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
      var GRAY = 'rgb(107, 107, 107)',
          BLUE = 'rgb(53, 53, 161)',
          RED = 'rgb(161, 53, 53)';

      // No attributes visualized.
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.edges).toConcur([
          { color: '', label: '', width: '>2' },
          { color: '', label: '', width: '>2' },
          { color: '', label: '', width: '>2' },
          { color: '', label: '', width: '>2' },
          ]);
        expect(graph.vertices).toConcur([
          { color: GRAY, icon: 'circle', label: '' },
          { color: GRAY, icon: 'circle', label: '' },
          { color: GRAY, icon: 'circle', label: '' },
          ]);
        savedPositions = positions(graph);
      });

      name.visualizeAs('label');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { label: 'Adam' },
          { label: 'Eve' },
          { label: 'Bob' },
          ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      gender.visualizeAs('icon');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { icon: 'male' },
          { icon: 'female' },
          { icon: 'male' },
          ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      income.visualizeAs('color');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { color: BLUE },
          { color: GRAY },
          { color: RED },
          ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      age.visualizeAs('size');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { size: '<15' },
          { size: '<15' },
          { size: '>15' },
          ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      age.visualizeAs('opacity');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { opacity: '<0.5' },
          { opacity: '<0.5' },
          { opacity: '1' },
          ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      age.visualizeAs('label-size');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { labelSize: '<15' },
          { labelSize: '<15' },
          { labelSize: '>15' },
          ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      age.visualizeAs('label-color');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { labelColor: 'rgb(66, 53, 161)' },
          { labelColor: BLUE },
          { labelColor: RED },
          ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      // There is no URL attribute in the example graph. Since we only check the "href"
      // attribute anyway, any string is good enough for the test.
      name.visualizeAs('image');
      lib.visualization.graphData().then(function(graph) {
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
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { opacity: '1', labelSize: '15', labelColor: '', image: null },
          { opacity: '1', labelSize: '15', labelColor: '', image: null },
          { opacity: '1', labelSize: '15', labelColor: '', image: null },
          ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      // Edge attributes.
      weight.visualizeAs('width');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.edges).toConcur([
          { width: '<6' },
          { width: '<6' },
          { width: '>6' },
          { width: '>6' },
        ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      weight.visualizeAs('edge-color');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.edges).toConcur([
          { color: BLUE },
          { color: 'rgb(125, 53, 161)' },
          { color: 'rgb(161, 53, 125)' },
          { color: RED },
        ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      comment.visualizeAs('edge-label');
      lib.visualization.graphData().then(function(graph) {
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
      lib.left.toggleSampledVisualization();
      lib.left.toggleSampledVisualization();
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { pos: { x: '>550', y: '>200' } },
          { pos: { x: '>550', y: '<200' } },
          { pos: { x: '<550', y: '<100' } },
          ]);
      });

      location.visualizeAs('geo-coordinates');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { pos: { x: '<500', y: '<100' } },
          { pos: { x: '>500', y: '<100' } },
          { pos: { x: '>600', y: '>100' } },
          ]);
      });

      lib.navigateToProject('test-example'); // Restore state.
    });

  fw.statePreservingTest(
    'test-example project with example graph',
    'visualize as slider',
    function() {
      addConcurMatcher();
      lib.left.toggleSampledVisualization();
      name.visualizeAs('label');
      age.visualizeAs('slider');
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
      var slider = age.slider();
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

      gender.visualizeAs('x');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur([
          { src: 0, dst: 1, width: '<6' },
          { src: 1, dst: 0, width: '>6' },
          { src: 1, dst: 1, width: '<6' },
          ]);
        expect(graph.vertices).toConcur([
          { label: '1' },
          { label: '3' },
          ]);
      });

      age.visualizeAs('y');
      lib.visualization.graphData().then(function(graph) {
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

      lib.navigateToProject('test-example'); // Restore state.
    });

  fw.statePreservingTest(
    'test-example project with example graph',
    'visualization for two open projects',
    function() {
      addConcurMatcher();
      lib.left.toggleSampledVisualization();
      name.visualizeAs('label');
      var leftPositions;
      lib.visualization.graphData().then(function(graph) {
        leftPositions = positions(graph);
        expect(graph.vertices.length).toBe(3);
      });

      lib.right.openSecondProject('test-example');
      lib.right.toggleBucketedVisualization();
      lib.right.vertexAttribute('gender').visualizeAs('y');
      lib.visualization.graphData().then(function(graph) {
        // Make sure the original vertices did not move.
        function matchPos(a, b) {
          return a.x.toFixed(3) === b.x.toFixed(3) && a.y.toFixed(3) === b.y.toFixed(3);
        }
        var pos = positions(graph);
        for (var i = 0; i < leftPositions.length; ++i) {
          var found = false;
          for (var j = 0; j < pos.length; ++j) {
            if (matchPos(pos[j], leftPositions[i])) {
              found = true;
              break;
            }
          }
          expect(found).toBe(true);
        }
        expect(graph.edges).toConcur([
          { src: 0, dst: 1, width: '<6' },
          { src: 0, dst: 3, width: '<6' },
          { src: 1, dst: 0, width: '<6' },
          { src: 1, dst: 4, width: '<6' },
          { src: 2, dst: 0, width: '<6' },
          { src: 2, dst: 1, width: '<6' },
          { src: 2, dst: 3, width: '<6' },
          { src: 2, dst: 4, width: '<6' },
          { src: 3, dst: 0, width: '<6' },
          { src: 3, dst: 4, width: '<6' },
          { src: 4, dst: 0, width: '<6' },
          { src: 4, dst: 1, width: '>6' },
          { src: 4, dst: 3, width: '>6' },
          { src: 4, dst: 4, width: '<6' },
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
      var expectedTSV = fs.readFileSync(__dirname + '/data/visualization-tsv-data.txt', 'utf8');
      expect(lib.visualization.asTSV()).toEqual(expectedTSV);

      lib.navigateToProject('test-example'); // Restore state.
    });

  fw.statePreservingTest(
    'test-example project with example graph',
    'visualization context menu',
    function() {
      addConcurMatcher();
      lib.left.toggleSampledVisualization();
      name.visualizeAs('label');
      lib.visualization.elementByLabel('Eve').click();
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

      lib.visualization.elementByLabel('Adam').click();
      lib.visualization.clickMenu('remove-from-centers');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.vertices).toConcur([
          { label: 'Eve' },
        ]);
        expect(graph.edges).toEqual([]);
      });

      lib.navigateToProject('test-example'); // Restore state.
    });

  var saveVisualizationOpen = lib.left.side.$('#save-visualization-dialog #text-dialog-open');
  var saveVisualizationEntry = lib.left.side.$('#save-visualization-dialog #dialogInput');
  var saveVisualizationOk = lib.left.side.$('#save-visualization-dialog #text-dialog-ok');
  var pickButton = lib.left.side.element(by.id('pick-and-next-button'));
  var centerCount = lib.left.side.element(by.id('pick-center-count'));

  fw.transitionTest(
    'test-example project with example graph',
    'visualization save/restore',
    function() {
      addConcurMatcher();
      lib.left.toggleSampledVisualization();
      // Set centers count to a non-default value.
      centerCount.clear();
      centerCount.sendKeys('2');
      pickButton.click();
      // Visualize names as labels.
      name.visualizeAs('label');
      // Save the visualization with the name 'my visualization'
      saveVisualizationOpen.click();
      saveVisualizationEntry.clear();
      saveVisualizationEntry.sendKeys('my visualization');
      saveVisualizationOk.click();
      // Close and reopen the project.
      lib.left.close();
      lib.splash.openProject('test-example');
      // Try loading the visualization and check if centers count is correctly updated.
      lib.left.toggleSampledVisualization();
      expect(centerCount.getAttribute('value')).toBe('1');
      lib.left.scalar('my-visualization').clickMenu('load-visualization');
      expect(centerCount.getAttribute('value')).toBe('2');
      // Check if the visualization is really loaded and not just the parameters.
      lib.visualization.graphData().then(function(graph) {
              expect(graph.vertices).toConcur([
                { label: 'Adam' },
                { label: 'Eve' },
                { label: 'Bob'},
              ]);
            });
    },
    function() {});
};
