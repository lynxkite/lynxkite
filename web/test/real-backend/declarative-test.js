'use strict';

var fw = (function UIDescription() {
  var states = {};
  var allStatePreservingTests = {};
  var hasChild = {};

  var mocks = require('../mocks.js');
  mocks.addTo(browser);
  browser.driver.manage().window().setSize(800, 600);

  return {
    transitionTest: function(
      previousStateName,  // Name of the state on which this transition should be applied.
      stateName,  // Name of the target state of this transition.
      transitionFunction,  // JS function that goes to this state from prev state.
      checks) {  // Tests confirming we are indeed in this state. Should be very fast stuff only,
                 // like looking at the DOM.
      var testingDone = false;
      hasChild[previousStateName] = true;

      function runStatePreservingTestInstance(name, runner) {
        it(name, function() {
          runner();
          // Checking that it was indeed statePreserving.
          checks();
        });
      }

      function runStatePreservingTestInstanceWithParam(name, runner, param) {
        runStatePreservingTestInstance(name, function() { runner(param); });
      }

      function runStatePreservingTest(currentTest) {
        if (!currentTest.instanceParams) {
          // Run this test once.
          runStatePreservingTestInstance(currentTest.name, currentTest.runTest);
        } else {
          // Run this test several times with different parameters.
          var params = currentTest.instanceParams;
          for (var i = 0; i < params.length; ++i) {
            runStatePreservingTestInstanceWithParam(
                currentTest.name + ' /' + i,
                currentTest.runTest,
                i);
          }
        }
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
    // instanceParams is an optional parameter which should be a list. If specified,
    // then the test will be run with each element of the list as a parameter.
    statePreservingTest: function(stateToRunAt, name, body, instanceParams) {
      if (allStatePreservingTests[stateToRunAt] === undefined) {
        allStatePreservingTests[stateToRunAt] = [];
      }
      allStatePreservingTests[stateToRunAt].push(
          {name: name, runTest: body, instanceParams: instanceParams});
    },

    runAll: function() {
      var stateNames = Object.keys(states);
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
  };
})();

require('./example-graph-basics.js')(fw);
require('./filter-tests.js')(fw);
require('./segmentation-opens.js')(fw);
require('./help-popups.js')(fw);
require('./histogram-tests.js')(fw);
require('./history-editor.js')(fw);
require('./undo-redo.js')(fw);
require('./workflow-tests.js')(fw);
require('./center-picker.js')(fw);
require('./splash-page.js')(fw);

fw.runAll();
