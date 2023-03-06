// Simple loading animation and error handling for inline elements, such as scalars.
// Does not generate any DOM once successfully loaded.
'use strict';
import '../app';
import './util';
import templateUrl from './inline-loading.html?url';

angular.module('biggraph').directive('inlineLoading', ['util', function(util) {
  return {
    restrict: 'E',
    scope: {
      ref: '=', // The resource we are loading.
      details: '=', // Additional information for the error report.
    },
    templateUrl,
    link: function(scope) {
      scope.util = util;
      scope.reportError = function() {
        util.reportRequestError(scope.ref, scope.details);
      };

      scope.iconForStatus = function(status, displayComputeButton) {
        if (!displayComputeButton) {
          // Use custom icon for some status codes.
          return {
            202: '\u2026', // 202 Accepted: ... (ellipsis)
          }[status] || '?';
        }
      };
    },
  };
}]);
