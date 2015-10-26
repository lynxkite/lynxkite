'use strict';

var lib = require('./test-lib.js');

module.exports = function(fw) {
  fw.statePreservingTest(
    'test-example project with example graph',
    'attribute visualizations',
    function() {
      jasmine.addMatchers({
        // A matcher for lists of objects that ignores fields not present in the reference.
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

      lib.left.toggleSampledVisualization();
      browser.waitForAngular();
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur([
          { src : 2, dst: 1 },
          { src : 1, dst: 0 },
          { src : 2, dst: 0 },
          { src : 0, dst: 1 },
          ]);
        expect(graph.vertices).toConcur([
          { color: 'rgb(107, 107, 107)', icon: 'circle', label: '' },
          { color: 'rgb(107, 107, 107)', icon: 'circle', label: '' },
          { color: 'rgb(107, 107, 107)', icon: 'circle', label: '' },
          ]);
      });

      lib.left.visualizeAttribute('gender', 'icon');
      browser.waitForAngular();
      lib.visualization.graphData().then(function(graph) {
        expect(graph.edges).toConcur([
          { src : 2, dst: 1 },
          { src : 1, dst: 0 },
          { src : 2, dst: 0 },
          { src : 0, dst: 1 },
          ]);
        expect(graph.vertices).toConcur([
          { icon: 'male' },
          { icon: 'female' },
          { icon: 'male' },
          ]);
      });

      lib.openProject('test-example'); // Restore state.
    });
};
