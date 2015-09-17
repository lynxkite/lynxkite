// Sends center requests with pagination support.
'use strict';

angular.module('biggraph').factory('getCenter', function(util) {
  function getCenter(resolvedParams, offset) {
    offset = offset || 0; // Default value.
    var count = resolvedParams.count;

    if (offset) {
      // Round up to multiples of 100.
      resolvedParams.count = 100 * Math.ceil((offset + count) / 100);
    }

    // We rely on the browser's cache to avoid re-sending requests for pagination.
    var req = util.get('/ajax/center', resolvedParams);
    req.$promise = req.$promise.then(
      function(result) {
        var centers = result.centers;
        offset = offset % centers.length;
        if (centers.length <= count) {
          // Use them all.
        } else if (centers.length < offset + count) {
          // Turn around.
          centers = centers.slice(offset).concat(centers).slice(0, count);
        } else {
          // Normal case.
          centers = centers.slice(offset, offset + count);
        }
        return centers;
      },
      function(response) {
        util.ajaxError(response);
      });

    return req;
  }

  return getCenter;
});
