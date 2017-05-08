// The links at the bottom of every page, such as "logout".
'use strict';

angular.module('biggraph').directive('bottomLinks', function($window, util) {
  return {
    restrict: 'E',
    scope: {
      info: '=',  // Debug data to post with "send feedback".
      dropup: '@', // Class selector for the dropup menu: "dropup" or "dropdown".
    },
    templateUrl: 'bottom-links.html',
    link: function(scope) {
      scope.util = util;

      scope.sendFeedback = function() {
        util.reportError({
          message: 'Click "report" to send an email about this page.',
          details: scope.info });
      };

      scope.logout = function() {
        util.post('/logout', {}).then(function() {
          $window.location.href = '/';
        });
      };
    },
  };
});
