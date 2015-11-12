'use strict';

var fw = (function UIDescription() {
  var states = {};
  var statePreservingTests = {};
  var hasChild = {};

  var mocks = require('./mocks.js');
  mocks.addTo(browser);
  browser.driver.manage().window().setSize(1100, 600);

  // If any tests are tagged with "solo", disables all other tests.
  function handleSolo() {
    // Adds "solo" to itself and all ancestors of a state.
    function soloAncestors(s) {
      s.tags.push('solo');
      if (s.parent !== undefined) {
        soloAncestors(states[s.parent]);
      }
    }

    var i, j, state, tests;
    var haveSolo = false;
    var stateNames = Object.keys(states);
    for (i = 0; i < stateNames.length; ++i) {
      state = states[stateNames[i]];
      if (state.tags.indexOf('solo') !== -1) {
        haveSolo = true;
        soloAncestors(state);
      }
      tests = statePreservingTests[stateNames[i]] || [];
      for (j = 0; j < tests.length; ++j) {
        if (tests[j].tags.indexOf('solo') !== -1) {
          haveSolo = true;
          soloAncestors(state);
        }
      }
    }

    if (haveSolo) {
      for (i = 0; i < stateNames.length; ++i) {
        state = states[stateNames[i]];
        if (state.tags.indexOf('solo') === -1) {
          state.tags.push('disabled');
        }
        tests = statePreservingTests[stateNames[i]] || [];
        for (j = 0; j < tests.length; ++j) {
          if (tests[j].tags.indexOf('solo') === -1) {
            tests[j].tags.push('disabled');
          }
        }
      }
    }
  }

  return {
    transitionTest: function(
      previousStateName,  // Name of the state on which this transition should be applied.
      stateName,  // Name of the target state of this transition.
      transitionFunction,  // JS function that goes to this state from prev state.
      checks,  // Tests confirming we are indeed in this state. Should be very fast stuff only,
               // like looking at the DOM.
      tags) {  // Space-separated list of tags to associate with the test.
      var testingDone = false;
      if (previousStateName !== undefined) {
        hasChild[previousStateName] = true;
      }

      function runStatePreservingTest(currentTest) {
        if (currentTest.tags.indexOf('disabled') !== -1) {
          return;
        }
        it('-- ' + currentTest.name, function() {
          currentTest.runTest();
          // Checking that it was indeed statePreserving.
          checks();
        });
      }

      states[stateName] = {
        tags: tags ? tags.split(' ') : [],
        parent: previousStateName,
        reachAndTest: function() {
          if (this.tags.indexOf('disabled') !== -1) {
            return;
          }
          if (previousStateName !== undefined) {
            states[previousStateName].reachAndTest();
          }
          describe(stateName, function() {
            it('can be reached', function() {
              transitionFunction();
              checks();
            });
            if (!testingDone) {
              var tests = statePreservingTests[stateName] || [];
              for (var i = 0; i < tests.length; i++) {
                runStatePreservingTest(tests[i]);
              }
              testingDone = true;
            }
          });
        },
      };
    },

    // These tests need to preserve the UI state or restore it when they are finished.
    statePreservingTest: function(stateToRunAt, name, body, tags) {
      if (statePreservingTests[stateToRunAt] === undefined) {
        statePreservingTests[stateToRunAt] = [];
      }
      statePreservingTests[stateToRunAt].push({
        name: name,
        runTest: body,
        tags: tags ? tags.split(' ') : [],
      });
    },

    runAll: function() {
      var stateNames = Object.keys(states);
      describe('Test setup', function() {
        it('defines all referenced test states', function() {
          var references = Object.keys(statePreservingTests).concat(Object.keys(hasChild));
          for (var i = 0; i < references.length; ++i) {
            expect(stateNames).toContain(references[i]);
          }
        });
      });
      handleSolo();
      for (var i = 0; i < stateNames.length; i++) {
        var stateName = stateNames[i];
        var state = states[stateName];
        // We only need to directly trigger testing for leaf nodes of the dependency trees as
        // states with children will be triggered by their children.
        if (!hasChild[stateName]) {
          state.reachAndTest();
        }
      }
    },

    cleanup: function() {
      describe('Cleanup', function() {
        it('temporary files', function() {
          var lib = require('./test-lib.js');
          var fs = require('fs');
          var pattern = lib.theRandomPattern;

          fs.readdir('/tmp', function (error, files) {
            if (error) {
              throw error;
            }
            for (var i = 0; i < files.length; i++) {
              var f = files[i];
              if (f.indexOf(pattern) > -1) {
                var full = '/tmp/' + f;
                console.log('Deleting: ' + full);
                fs.unlink(full);
              }
            }
          });
        });
      });
    },
  };
})();

var fs = require('fs');
var testFiles = fs.readdirSync(__dirname + '/tests');
for (var i = 0; i < testFiles.length; ++i) {
  if (testFiles[i].slice(-3) === '.js') {
    require('./tests/' + testFiles[i])(fw);
  }
}

fw.runAll();
// Use the below line to only run one transition test and its
// state-preserving descendants.
// fw.runOne('NAME OF A TRANSITION TEST');
fw.cleanup();
