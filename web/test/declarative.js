'use strict';

var fw = (function UIDescription() {
  var states = {};
  var statePreservingTests = {};
  var soloMode = false;

  var mocks = require('./mocks.js');
  mocks.addTo(browser);
  browser.driver.manage().window().setSize(1100, 600);

  return {
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
          currentTest.runTest();
          // Checking that it was indeed statePreserving.
          checks();
        });
      }

      states[stateName] = {
        parent: previousStateName,
        mustBeReached: function() {
          if (!soloMode) return true;
          if (solo) return true;
          var tests = statePreservingTests[stateName] || [];
          for (var j = 0; j < tests.length; ++j) {
            if (tests[j].solo) return true;
          }
          return false;
        },
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
        var state = states[stateName]
        var parent = state.parent
        if (parent !== undefined) {
          if (!statesAutomaticallyReached[parent]) {
            statesAutomaticallyReached[parent] = true;
            markParentsAutomaticallyReached(parent);
          }
        }
      }

      for (var i = 0; i < stateNames.length; i++) {
        var stateName = stateNames[i];
        var state = states[stateName];
        if (state.mustBeReached()) {
          statesToReach.push(stateName);
          markParentsAutomaticallyReached(stateName);
        }
      }
      for (var i = 0; i < statesToReach.length; i++) {
        var stateName = statesToReach[i];
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

          fs.readdir(lib.protractorDownloads, function (error, files) {
            if (error) {
              throw error;
            }
            for (var i = 0; i < files.length; i++) {
              var f = files[i];
              if (f.indexOf(pattern) > -1) {
                var full = lib.protractorDownloads + '/' + f;
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


var startDate = (new Date()).toString();
var screenshots = [];
var screenshotDir = '/tmp/';
var userVisiblePrefix = screenshotDir;
try {
  var userContentDir = process.env.HOME + '/userContent';
  // This throws an exception is the fs entry does not exist at all.
  var stats = fs.lstatSync(userContentDir);
  if (stats.isDirectory()) {
    screenshotDir = userContentDir + '/';
    userVisiblePrefix = 'http://' + require("os").hostname() + ':8888/userContent/';
  }
} catch (e) {
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

fw.runAll();

fw.cleanup();

describe('The test framework ', function() {
  it('now prints all screenshots', function() {
    if (screenshots.length > 0) {
      console.log('\nError screenshots:');
      for (i = 0; i < screenshots.length; ++i) {
        console.log(screenshots[i]);
      }
    }
  });
});
