'use strict';

angular.module('biggraph').directive('bottomLinks', function($location, util) {
  return {
    restrict: 'E',
    scope: { info: '=' },  // Debug data to post with "send feedback".
    templateUrl: 'bottom-links.html',
    link: function(scope) {
      scope.util = util;

      scope.sendFeedback = function() {
        util.reportError({
          message: 'Click &quot;report&quot; to send an email about this page.',
          details: scope.info });
      };

      scope.logout = function() {
        util.post('/logout', {}, function() {
          $location.url('/');
        });
      };
    },
  };
});
