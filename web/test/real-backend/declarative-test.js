'use strict';

var fw = (function UIDescription() {
  var states = {};
  var allStatePreservingTests = {};
  var hasChild = {};

  var mocks = require('../mocks.js');
  mocks.addTo(browser);
  browser.driver.manage().window().setSize(1100, 600);

  return {
    transitionTest: function(
      previousStateName,  // Name of the state on which this transition should be applied.
      stateName,  // Name of the target state of this transition.
      transitionFunction,  // JS function that goes to this state from prev state.
      checks) {  // Tests confirming we are indeed in this state. Should be very fast stuff only,
                 // like looking at the DOM.
      var testingDone = false;
      if (previousStateName !== undefined) {
        hasChild[previousStateName] = true;
      }

      function runStatePreservingTest(currentTest) {
        it('-- ' + currentTest.name, function() {
          currentTest.runTest();
          // Checking that it was indeed statePreserving.
          checks();
        });
      }

      states[stateName] = {
        reachAndTest: function() {
          if (previousStateName !== undefined) {
            states[previousStateName].reachAndTest();
          }
          describe(stateName, function() {
            it('can be reached', function() {
              transitionFunction();
              checks();
            });
            if (!testingDone) {
              var statePreservingTests = allStatePreservingTests[stateName] || [];
              for (var i = 0; i < statePreservingTests.length; i++) {
                runStatePreservingTest(statePreservingTests[i]);
              }
              testingDone = true;
            }
          });
        },
      };
    },

    // These tests need to preserve the UI state or restore it when they are finished.
    statePreservingTest: function(stateToRunAt, name, body) {
      if (allStatePreservingTests[stateToRunAt] === undefined) {
        allStatePreservingTests[stateToRunAt] = [];
      }
      allStatePreservingTests[stateToRunAt].push({name: name, runTest: body});
    },

    runAll: function() {
      var stateNames = Object.keys(states);
      describe('Test setup', function() {
        it('defines all referenced test states', function() {
          var references = Object.keys(allStatePreservingTests).concat(Object.keys(hasChild));
          for (var i = 0; i < references.length; ++i) {
            expect(stateNames).toContain(references[i]);
          }
        });
      });
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

    runOne: function(stateName) {
      states[stateName].reachAndTest();
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

require('./example-graph-basics.js')(fw);
require('./download-test.js')(fw);
require('./upload-test.js')(fw);
require('./filter-tests.js')(fw);
require('./segmentation-opens.js')(fw);
require('./help-popups.js')(fw);
require('./histogram-tests.js')(fw);
require('./history-editor.js')(fw);
require('./undo-redo.js')(fw);
require('./workflow-tests.js')(fw);
require('./center-picker.js')(fw);
require('./splash-page.js')(fw);
require('./errors.js')(fw);
require('./visualization.js')(fw);
require('./operations.js')(fw);

fw.runAll();
// Use the below line to only run one transition test and its
// state-preserving descendants.
// fw.runOne('NAME OF A TRANSITION TEST');
fw.cleanup();
