'use strict';

var fw = (function UIDescription() {
  var states = {};
  var statePreservingTests = {};
  var hasChild = {};
  var hanSoloMode = false;

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
      hanSolo) {  // Set to true if you want to run only this test.
      if (hanSolo) {
        hanSoloMode = true;
      }
      var testingDone = false;
      var implicitlyReached = false;
      if (previousStateName !== undefined) {
        hasChild[previousStateName] = true;
      }

      function runStatePreservingTest(currentTest) {
        if (hanSoloMode && !currentTest.hanSolo) {
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
        isHanSolo: function() {
          if (hanSolo) return true;
          var tests = statePreservingTests[stateName] || [];
          for (var j = 0; j < tests.length; ++j) {
            if (tests[j].hanSolo) return true;
          }
          return false;
        },
        needsExplicitReach: function() {
          if (implicitlyReached) return false;
          return !hanSoloMode || this.isHanSolo();
        },
        reportReach: function() {
          if (previousStateName !== undefined) {
            states[previousStateName].reportImplicitReach();
          }
        },
        reportImplicitReach: function() {
          implicitlyReached = true;
          if (previousStateName !== undefined) {
            states[previousStateName].reportImplicitReach();
          }
        },
        reachAndTest: function() {
          if (previousStateName !== undefined) {
            states[previousStateName].reachAndTest(true);
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
    statePreservingTest: function(stateToRunAt, name, body, hanSolo) {
      if (hanSolo) {
        hanSoloMode = true;
      }
      if (statePreservingTests[stateToRunAt] === undefined) {
        statePreservingTests[stateToRunAt] = [];
      }
      statePreservingTests[stateToRunAt].push({
        name: name,
        runTest: body,
        hanSolo: hanSolo,
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

      // We don't want to reach any state more times than necessary. That is, if a state is
      // reached because we had to reach one of its offsprings, then we won't need to explicitly
      // reach that state again. To achieve this we mark all ancestors of states that we need to
      // reach and don't try to reach them explicitly in the following loop.
      for (var i = 0; i < stateNames.length; i++) {
        var stateName = stateNames[i];
        var state = states[stateName];
        if (state.needsExplicitReach()) {
          state.reportReach(state.parent);
        }
      }
      for (var i = 0; i < stateNames.length; i++) {
        var stateName = stateNames[i];
        var state = states[stateName];
        // We only need to directly trigger testing for leaf nodes of the dependency trees as
        // states with children will be triggered by their children.
        if (state.needsExplicitReach()) {
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
    if (screenshots) {
      console.log('\nError screenshots:');
      for (i = 0; i < screenshots.length; ++i) {
        console.log(screenshots[i]);
      }
    }
  });
});
