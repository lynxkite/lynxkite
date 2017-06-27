'use strict';
/* eslint-disable no-console */

function UIDescription() {
  var states = {};
  var statePreservingTests = {};
  var soloMode = false;
  var verboseMode = true;

  var mocks = require('./mocks.js');
  mocks.addTo(browser);
  browser.driver.manage().window().setSize(1100, 700);

  return {
    isSolo: () => soloMode,
    setSolo: (s) => { soloMode = s; },
    transitionTest: function(
      previousStateName,  // Name of the state on which this transition should be applied.
      stateName,  // Name of the target state of this transition.
      transitionFunction,  // JS function that goes to this state from prev state.
      checks,  // Tests confirming we are indeed in this state. Should be very fast stuff only,
               // like looking at the DOM.
      solo) {  // Set to true if you want to run only this test.
      if (solo) {
        soloMode = true;
      }
      var testingDone = false;

      function runStatePreservingTest(currentTest) {
        if (soloMode && !currentTest.solo) {
          return;
        }
        it('-- ' + currentTest.name, function() {
          if (verboseMode) { console.log(' - ' + currentTest.name); }
          currentTest.runTest();
          // Checking that it was indeed statePreserving.
          checks();
        });
      }

      states[stateName] = {
        parent: previousStateName,
        mustBeReached: function() {
          if (!soloMode) { return true; }
          if (solo) { return true; }
          var tests = statePreservingTests[stateName] || [];
          for (var j = 0; j < tests.length; ++j) {
            if (tests[j].solo) { return true; }
          }
          return false;
        },
        reachAndTest: function() {
          if (previousStateName !== undefined) {
            states[previousStateName].reachAndTest();
          }
          describe(stateName, function() {
            it('can be reached', function() {
              if (verboseMode) { console.log(stateName); }
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
    statePreservingTest: function(stateToRunAt, name, body, solo) {
      if (solo) {
        soloMode = true;
      }
      if (statePreservingTests[stateToRunAt] === undefined) {
        statePreservingTests[stateToRunAt] = [];
      }
      statePreservingTests[stateToRunAt].push({
        name: name,
        runTest: body,
        solo: solo,
      });
    },

    runAll: function() {
      var stateNames = Object.keys(states);
      // We will enumerate all states we want to visit here.
      var statesToReach = [];
      // We put here all states that we visit inevitably, that is parents of states in
      // statesToReach.
      var statesAutomaticallyReached = {};
      function markParentsAutomaticallyReached(stateName) {
        var state = states[stateName];
        var parent = state.parent;
        if (parent !== undefined) {
          if (!statesAutomaticallyReached[parent]) {
            statesAutomaticallyReached[parent] = true;
            markParentsAutomaticallyReached(parent);
          }
        }
      }

      var i, stateName;
      for (i = 0; i < stateNames.length; i++) {
        stateName = stateNames[i];
        var state = states[stateName];
        if (state.mustBeReached()) {
          statesToReach.push(stateName);
          markParentsAutomaticallyReached(stateName);
        }
      }
      for (i = 0; i < statesToReach.length; i++) {
        stateName = statesToReach[i];
        if (!statesAutomaticallyReached[stateName]) {
          states[stateName].reachAndTest();
        }
      }
    },

    cleanup: function() {
      describe('Cleanup', function() {
        it('temporary files', function() {
          var lib = require('./test-lib.js');
          var fs = require('fs');
          var pattern = lib.theRandomPattern;

          var downloads = lib.protractorDownloads;
          var files = fs.readdirSync(downloads);
          for (var i = 0; i < files.length; i++) {
            var f = files[i];
            if (f.indexOf(pattern) > -1) {
              var full = downloads + '/' + f;
              console.log('Deleting: ' + full);
              fs.unlinkSync(full);
            }
          }
          console.log('Deleting: ' + downloads);
          fs.rmdirSync(downloads);
        });

        it('fails in solo mode so it is not accidentally committed', function() {
          expect(soloMode).toBe(false);
        });
      });
    },
  };
}

var fs = require('fs');
function testsFrom(testsDir) {
  var fw = new UIDescription();
  var testFiles = fs.readdirSync(__dirname + '/' + testsDir);
  for (var i = 0; i < testFiles.length; ++i) {
    if (testFiles[i].slice(-3) === '.js') {
      require('./' + testsDir + '/' + testFiles[i])(fw);
    }
  }
  return fw;
}
var authFw = testsFrom('auth-tests');
var authlessFw = testsFrom('tests');
// Set 'solo' modes.
// Authentication tests are unaffected by authless 'solo' labels, because
// all the authless tests depend on the login performed by the auth tests.
authFw.setSolo(authFw.isSolo());
authlessFw.setSolo(authFw.isSolo() || authlessFw.isSolo());

var startDate = (new Date()).toString();
var screenshots = [];
var screenshotDir = '/tmp/';
var userVisiblePrefix = screenshotDir;
try {
  var userContentDir = process.env.HOME + '/userContent';
  // This throws an exception if the fs entry does not exist at all.
  var stats = fs.lstatSync(userContentDir);
  if (stats.isDirectory()) {
    screenshotDir = userContentDir + '/';
    userVisiblePrefix = 'http://jenkins/userContent/';
  }
} catch (e) {
  console.error(e, e.stack);
}

// Makes a screenshot if an expectation fails.
var originalAddExpectationResult = jasmine.Spec.prototype.addExpectationResult;
jasmine.Spec.prototype.addExpectationResult = function() {
  if (!arguments[0]) {
    var that = this;
    browser.takeScreenshot().then(function(png) {
      var failureIdx = that.failedExpectations || 0;
      that.failedExpectations = failureIdx + 1;
      var filename = (
        ('protractor-' + startDate + '-' + that.getFullName() + '-' + failureIdx + '.png')
        .replace(/[^a-z0-9.-]/gi, '_')
        .toLowerCase());
      screenshots.push(userVisiblePrefix + filename);
      var stream = fs.createWriteStream(screenshotDir + filename);
      stream.write(new Buffer(png, 'base64'));
      stream.end();
    });
  }
  return originalAddExpectationResult.apply(this, arguments);
};

console.log('Starting tests at: ' + startDate);

// After the authentication tests you are logged in, so you don't need to log
// out and in every time you reach the empty splash state.
if (process.env.HTTPS_PORT) {
  authFw.runAll();
}
authlessFw.runAll();
authlessFw.cleanup();

describe('The test framework ', function() {
  it('now prints all screenshots', function() {
    if (screenshots.length > 0) {
      console.log('\nError screenshots:');
      for (var i = 0; i < screenshots.length; ++i) {
        console.log(screenshots[i]);
      }
    }
  });
});
