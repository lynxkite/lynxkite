'use strict';

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

  var expectedEdges = [
    { src : 0, dst: 1 },
    { src : 1, dst: 0 },
    { src : 2, dst: 0 },
    { src : 2, dst: 1 },
  ];

  var positions;
  function savePositions(graph) {
    positions = [];
    for (var i = 0; i < graph.vertices.length; ++i) {
      positions.push(graph.vertices[i].pos);
    }
  }
  function expectPositions(graph) {
    expect(graph.vertices.length).toBe(positions.length);
    for (var i = 0; i < graph.vertices.length; ++i) {
      expect(graph.vertices[i].pos).toBe(positions[i]);
    }
  }

  fw.statePreservingTest(
    'test-example project with example graph',
    'attribute visualizations',
    function() {
      addConcurMatcher();
      lib.left.toggleSampledVisualization();

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
        savePositions(graph);
      });

      lib.left.visualizeAttribute('name', 'label');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { label: 'Adam' },
          { label: 'Eve' },
          { label: 'Bob' },
          ]);
        expectPositions(graph);
      });

      lib.left.visualizeAttribute('gender', 'icon');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { icon: 'male' },
          { icon: 'female' },
          { icon: 'male' },
          ]);
        expectPositions(graph);
      });

      lib.left.visualizeAttribute('income', 'color');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { color: 'rgb(53, 53, 161)' },
          { color: 'rgb(107, 107, 107)' },
          { color: 'rgb(161, 53, 53)' },
          ]);
        expectPositions(graph);
      });

      lib.left.visualizeAttribute('age', 'size');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { size: '12.858032957292307' },
          { size: '12.17481048468403' },
          { size: '20.240000000000002' },
          ]);
        expectPositions(graph);
      });

      lib.left.visualizeAttribute('age', 'opacity');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { opacity: '0.4035785288270378' },
          { opacity: '0.36182902584493043' },
          { opacity: '1' },
          ]);
        expectPositions(graph);
      });

      lib.left.visualizeAttribute('age', 'label-size');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { labelSize: '12.107355864811135px' },
          { labelSize: '10.854870775347912px' },
          { labelSize: '30px' },
          ]);
        expectPositions(graph);
      });

      lib.left.visualizeAttribute('age', 'label-color');
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
        expect(graph.vertices).toConcur([
          { labelColor: 'rgb(66, 53, 161)' },
          { labelColor: 'rgb(53, 53, 161)' },
          { labelColor: 'rgb(161, 53, 53)' },
          ]);
        expectPositions(graph);
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
        expectPositions(graph);
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
        expectPositions(graph);
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
        expectPositions(graph);
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
        expectPositions(graph);
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
        expectPositions(graph);
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
};
