'use strict';

module.exports = function() {};

// var fs = require('fs');
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
    for (var i = 0; i < saved.length; ++i) {
      expect(saved[i].x).toBeCloseTo(graph[i].x, precision);
      expect(saved[i].y).toBeCloseTo(graph[i].y, precision);
    }
  }
  var visualization = lib.workspace.getStateView('vz0', 'visualization').visualization;

  var editor = lib.workspace.getVisualizationEditor('vz0');
  var name = editor.left.vertexAttribute('name');
  var gender = editor.left.vertexAttribute('gender');
  var income = editor.left.vertexAttribute('income');
  var age = editor.left.vertexAttribute('age');
  var location = editor.left.vertexAttribute('location');
  var weight = editor.left.edgeAttribute('weight');
  var comment = editor.left.edgeAttribute('comment');

  fw.transitionTest(
    'test-example workspace with example graph',
    'test-example workspace with visualization open',
    function() {
      lib.workspace.addBox({
        id: 'sg0',
        name: 'Segment by Double attribute',
        x: 100, y: 200,
        after: 'eg0',
        params: { attr: 'income', interval_size: '10'},
      });
      lib.workspace.addBox({
        id: 'vz0',
        name: 'Create visualization',
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

  fw.statePreservingTest(
    'test-example workspace with visualization open',
    'sampled mode attribute visualizations',
    function() {
      addConcurMatcher();
      //editor.left.toggleSampledVisualization();

      var expectedEdges = [
        { src: 0, dst: 1 },
        { src: 1, dst: 0 },
        { src: 2, dst: 0 },
        { src: 2, dst: 1 },
      ];
      var savedPositions;
      var GRAY = 'rgb(107, 107, 107)';
      var BLUE = 'rgb(53, 53, 161)';
      var RED = 'rgb(161, 53, 53)';
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
          { color: GRAY, icon: 'circle', label: '' },
          { color: GRAY, icon: 'circle', label: '' },
          { color: GRAY, icon: 'circle', label: '' },
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
          { color: BLUE },
          { color: GRAY },
          { color: RED },
        ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      age.visualizeAs('size');
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { size: '<30' },
          { size: '<30' },
          { size: '>30' },
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
          { labelColor: 'rgb(66, 53, 161)' },
          { labelColor: BLUE },
          { labelColor: RED },
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
          { opacity: '1', labelSize: '15', labelColor: '', image: null },
          { opacity: '1', labelSize: '15', labelColor: '', image: null },
          { opacity: '1', labelSize: '15', labelColor: '', image: null },
        ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      // Edge attributes.
      weight.visualizeAs('width');
      visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.edges).toConcur([
          { width: '<12' },
          { width: '<12' },
          { width: '>12' },
          { width: '>12' },
        ]);
        checkGraphPositions(positions(graph), savedPositions);
      });

      weight.visualizeAs('edge-color');
      visualization.graphData().then(function(graph) {
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
          { pos: { x: '>200', y: '>300' } },
          { pos: { x: '>200', y: '<300' } },
          { pos: { x: '<200', y: '<200' } },
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

      //lib.navigateToProject('test-example'); // Restore state.
    });
/*
  fw.statePreservingTest(
    'test-example workspace with example graph',
    'visualize as slider',
    function() {
      addConcurMatcher();
      lib.left.toggleSampledVisualization();
      name.visualizeAs('label');
      age.visualizeAs('slider');
      var RED = 'rgb(161, 53, 53)';
      var YELLOW = 'rgb(184, 184, 46)';
      var GREEN = 'rgb(53, 161, 53)';
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
    'test-example workspace with example graph',
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
    'test-example workspace with example graph',
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
*/
/*
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

  var saveVisualizationOpen = lib.left.side.$('#setting-save-visualization');
  var saveVisualizationEntry = $('#save-visualization-name');
  var centersToken = lib.left.side.$('#setting-centers');
  var pickButton = $('#pick-and-next-button');
  var centerCount = $('#pick-center-count');

  fw.transitionTest(
    'test-example project with example graph',
    'visualization save/restore',
    function() {
      addConcurMatcher();
      lib.left.toggleSampledVisualization();
      // Set centers count to a non-default value.
      centersToken.click();
      centerCount.clear();
      centerCount.sendKeys('2');
      pickButton.click();
      // Visualize names as labels.
      name.visualizeAs('label');
      // Save the visualization with the name 'my visualization'
      saveVisualizationOpen.click();
      saveVisualizationEntry.sendKeys('my visualization');
      saveVisualizationEntry.submit();
      // Close and reopen the project.
      lib.left.close();
      lib.splash.openProject('test-example');
      // Try loading the visualization and check if centers count is correctly updated.
      lib.left.toggleSampledVisualization();
      centersToken.click();
      expect(centerCount.getAttribute('value')).toBe('1');
      lib.left.scalar('my-visualization').clickMenu('load-visualization');
      centersToken.click();
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
*/
};
