var fw = (function UIDescription() {
  var states = {};
  var allIdempotentTests = {};
  var hasChild = {};

  states['empty splash'] = {
    goToState: function(lib) {
      browser.driver.get('http://localhost:9000/ajax/discardAllReallyIMeanIt?q=%7B"fake"%3A1%7D');
      browser.get('/');
    },
    done: function() {
      return true;
    },
  }

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
      states[stateName] = {
        goToState: function() {
          states[previousStateName].goToState();
          describe(stateName, function() {
            it('can be reached', function() {
              transitionFunction(lib);
              checks(lib);
            });
            if (!testingDone) {
              idempotentTests = allIdempotentTests[stateName] || [];
              for (i = 0; i < idempotentTests.length; i++) {
                var currentTest = idempotentTests[i]
                it(currentTest.name, function() {
                  currentTest.runTest(lib);
                  // Checking that it was indeed idempotent.
                  checks(lib);
                });
              }
              testingDone = true;
            }
          });
        },
        done: function() {
          return testingDone;
        },
      }
    },
    newIdempotentTest: function(stateToRunAt, name, body) {
      if (allIdempotentTests[stateToRunAt] === undefined) {
        allIdempotentTests[stateToRunAt] = [];
      }
      allIdempotentTests[stateToRunAt].push({name: name, runTest: body});
    },
    runAll: function() {
      for (var stateName in states) {
        if (states.hasOwnProperty(stateName)) {
          var state = states[stateName];
          if (!hasChild[stateName] && !state.done()) {
            state.goToState();
          }
        }
      }
    },
  };
})();


require('./example-graph-basics.js')(fw);

fw.runAll();




