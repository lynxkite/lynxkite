'use strict';

const lib = require('../test-lib.js');
const request = require('request');
const url = require('url');

function authenticateAndThen(requestDescription) {
  return lib.authenticateAndPost('admin', 'adminpw', 'lynxkite',
    defer => {
      let req = request.defaults({ jar: true });
      req(requestDescription, (error, response, body) => defer.fulfill(body));
    }
  );
}

module.exports = function(fw) {
  fw.statePreservingTest(
    'empty splash', 'logfiles get rotated',
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
      function getLogFilesCount() {
        return authenticateAndThen(getLogFiles).then(body => JSON.parse(body).files.length);
      }
      let numLogFilesBefore;
      getLogFilesCount().then(logFilesCount => numLogFilesBefore = logFilesCount)
        .then(() => authenticateAndThen(forceLogRotate))
        .then(() => getLogFilesCount())
        .then(numLogFilesAfter => expect(numLogFilesAfter - numLogFilesBefore).toEqual(1));
    });

  fw.statePreservingTest(
    'empty splash', 'dynamic content is escaped in error messages',
    function() {
      let uri = url.parse(browser.baseUrl);
      uri.path = '/<script>cross_site_scripting.nasl</script>.asp';
      let nonexistentEndpoint = {
        uri: uri
      };
      let error = authenticateAndThen(nonexistentEndpoint);
      expect(error).toContain('&lt;script&gt;cross_site_scripting.nasl&lt;');
    });
};
