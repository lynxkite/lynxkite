// The links at the bottom of every page, such as "logout".
'use strict';

angular.module('biggraph').directive('userMenu', function($window, util, $rootScope) {
  return {
    restrict: 'E',
    scope: {
      info: '=', // Debug data to post with "send feedback".
      direction: '@', // Class selector for the dropup menu: "dropup" or "dropdown".
    },
    templateUrl: 'scripts/util/user-menu.html',
    link: function(scope) {
      scope.util = util;

      scope.sendFeedback = function() {
        util.reportError({
          message: 'Click "send feedback" to send an email.',
          details: scope.info });
      };

      scope.restartApplication = function() {
        util.post('/ajax/restartApplication', {})
          .finally(function() { $window.location.reload(); });
      };

      scope.logout = function() {
        util.post('/logout', {}).then(function() {
          $window.location.href = '/';
        });
      };

      scope.tutorial = function() {
        $rootScope.$broadcast('start tutorial');
      };
    },
  };
});
