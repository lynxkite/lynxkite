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
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur(expectedEdges);
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
