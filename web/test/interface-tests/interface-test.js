'use strict';

const lib = require('../test-lib.js');
const request = require('request');

function authenticateAndThen(requestDescription) {
  return lib.authenticateAndPost('admin', 'adminpw', 'lynxkite',
    defer => {
      let req = request.defaults({ jar: true });
      req(requestDescription, (error, response, body) => defer.fulfill(body));
    }
  );
}

module.exports = function(fw) {
  fw.transitionTest(
    undefined, 'logfiles get rotated',
    function() {
      let getLogFiles = {
        method: 'GET',
        // we don't seem to care about the q=1 parameter but the request fails without it:
        // https://github.com/biggraph/biggraph/blob/master/app/com/lynxanalytics/biggraph/serving/JsonServer.scala#L92
        url: browser.baseUrl + 'getLogFiles?q=1',
        headers: {
          'X-Requested-With': 'XMLHttpRequest'
        },
        // without `strictSSL` set to false we can not make requests against self authenticated
        // hosts
        strictSSL: false
      };
      let forceLogRotate = {
        method: 'POST',
        url: browser.baseUrl + 'forceLogRotate',
        headers: {
          'Content-Type': 'application/json'
        },
        strictSSL: false,
        json: { fake: 0 }
      };
      let numLogFilesDiff = authenticateAndThen(getLogFiles)
        .then(body => JSON.parse(body).files.length)
        .then(numLogFilesBefore => {
          let notInterestingJustWaitingForItToResolve = authenticateAndThen(forceLogRotate);
          return Promise.all([numLogFilesBefore, notInterestingJustWaitingForItToResolve]);
        })
        .then(pair => {
          let numLogFilesBefore = pair[0];
          let numLogFilesAfter = authenticateAndThen(getLogFiles)
            .then(body => JSON.parse(body).files.length);
          return Promise.all([numLogFilesBefore, numLogFilesAfter]);
        })
        .then(pair => {
          let numLogFilesBefore = pair[0];
          let numLogFilesAfter = pair[1];
          return numLogFilesAfter - numLogFilesBefore;
        });
      expect(numLogFilesDiff).toEqual(1);
    }, function() {});

  fw.statePreservingTest(
    'logfiles get rotated', 'dynamic content is escaped in error messages',
    function() {
      let port = parseInt(browser.baseUrl.slice(-5, -1));
      let nonexistentEndpoint = {
        uri: {
          protocol:'https:',
          host: 'localhost',
          port: port,
          path: '/<script>cross_site_scripting.nasl</script>.asp'
        }
      };
      let error = authenticateAndThen(nonexistentEndpoint);
      expect(error).toContain('&lt;script&gt;cross_site_scripting.nasl&lt;');
    });
};
