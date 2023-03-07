import '../app';
import './util';
import templateUrl from './display-error.html?url';

// Viewer of an error on the UI.

angular.module('biggraph')
  .directive('displayError', ['util', function(util) {
    return {
      restrict: 'E',
      templateUrl,
      scope: {
        caption: '@', // Caption to display on the UI.
        request: '=', // The request which may result in an error. Takes precedence over 'error'.
        error: '=', // The error variable to bind and display. Use only if 'request' is not available.
      },
      link: function(scope) {
        scope.message = function() {
          if (scope.request) {
            return scope.request.$error;
          } else {
            return scope.error;
          }
        };
        scope.show = function() {
          return (scope.request && scope.request.$resolved && scope.request.$error) || scope.error;
        };
        scope.reportError = function() {
          if (scope.request) {
            util.reportRequestError(scope.request);
          } else {
            util.reportError({ message: scope.error });
          }
        };
      },
    };
  }]);
