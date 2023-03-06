// Sends center requests with pagination support.
'use strict';
import '../app';
import '../util/util';

angular.module('biggraph').factory('getCenter', ['util', function(util) {
  function getCenter(resolvedParams) {
    let offset = resolvedParams.offset || 0; // Default value.
    delete resolvedParams.offset; // This parameter is just for getCenter.
    const count = resolvedParams.count;

    if (offset) {
      // Round up to multiples of 100.
      resolvedParams.count = 100 * Math.ceil((offset + count) / 100);
    }

    // We rely on the browser's cache to avoid re-sending requests for pagination.
    const req = util.get('/ajax/center', resolvedParams);
    const promise = req.then(
      function(result) {
        let centers = result.centers;
        offset = offset % centers.length;
        if (centers.length <= count) {
          // Use them all.
        } else if (centers.length < offset + count) {
          // Turn around.
          centers = centers.concat(centers).slice(offset, offset + count);
        } else {
          // Normal case.
          centers = centers.slice(offset, offset + count);
        }
        promise.$resolved = true; // Pretend ngResource interface for loading animation.
        return centers;
      },
      function(error) {
        util.ajaxError(error);
      });

    return promise;
  }

  return getCenter;
}]);
