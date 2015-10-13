// Angular waits for $http requests and digests to stabilize.
// This plugin makes it wait for our slow requests queue to be empty too.

exports.waitForPromise = function() {
  return browser.executeAsyncScript(waitForSlowRequests);
};

// Runs inside the browser! Calls "done" when the slow requests queue is empty.
function waitForSlowRequests(done) {
  var el = document.querySelector('html');
  var $injector = angular.element(el).injector();
  var util = $injector.get('util');
  var $rootScope = $injector.get('$rootScope');
  $rootScope.$digest();
  if (util.slowQueue.queue.length === 0) {
    done();
  } else {
    var $q = $injector.get('$q');
    var l = util.slowQueue.queue.length;
    $q.all(util.slowQueue.queue).finally(function() {
      // Everything is done. Wait again for anything added in the meantime.
      waitForSlowRequests(done);
    });
  }
};
