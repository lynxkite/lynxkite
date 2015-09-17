'use strict';

var fw = (function UIDescription() {
  var states = {};
  var allIdempotentTests = {};
  var hasChild = {};

  states['empty splash'] = {
    reachAndTest: function() {
      browser.driver.get('http://localhost:9000/ajax/discardAllReallyIMeanIt?q=%7B"fake"%3A1%7D');
      browser.get('/');
    },
    done: function() {
      return true;
    },
  };

  var mocks = require('../mocks.js');
  var lib = require('./test-lib.js');
  mocks.addTo(browser);
  
  return {
    newUIState: function(
      stateName,  // Name of the state being defined.
      previousStateName,  // Name of the state from which this can be reached.
      transitionFunction,  // JS function that goes to this state from prev state.
      checks) {  // Tests confirming we are indeed in this state. Should be very fast stuff only,
                 // like looking at the DOM.
      var testingDone = false;
      hasChild[previousStateName] = true;

      function runIdempotentTest(currentTest) {
        it(currentTest.name, function() {
          currentTest.runTest(lib);
          // Checking that it was indeed idempotent.
          checks(lib);
        });
      }

      states[stateName] = {
        reachAndTest: function() {
          states[previousStateName].reachAndTest();
          describe(stateName, function() {
            it('can be reached', function() {
              transitionFunction(lib);
              checks(lib);
            });
            if (!testingDone) {
              var idempotentTests = allIdempotentTests[stateName] || [];
              for (var i = 0; i < idempotentTests.length; i++) {
                runIdempotentTest(idempotentTests[i]);
              }
              testingDone = true;
            }
          });
        },
        done: function() {
          return testingDone;
        },
      };
    },
    newIdempotentTest: function(stateToRunAt, name, body) {
      if (allIdempotentTests[stateToRunAt] === undefined) {
        allIdempotentTests[stateToRunAt] = [];
      }
      allIdempotentTests[stateToRunAt].push({name: name, runTest: body});
    },
    runAll: function() {
      var stateNames = Object.keys(states);
      for (var i = 0; i < stateNames.length; i++) {
        var stateName = stateNames[i];
        var state = states[stateName];
        if (!hasChild[stateName] && !state.done()) {
          state.reachAndTest();
        }
      }
    },
  };
})();


require('./example-graph-basics.js')(fw);

fw.runAll();




