// The links at the bottom of every page, such as "logout".
'use strict';
import '../app';
import './util';
import templateUrl from './user-menu.html?url';

angular.module('biggraph').directive('userMenu', function($window, util, $rootScope) {
  return {
    restrict: 'E',
    scope: {
      direction: '@', // Class selector for the dropup menu: "dropup" or "dropdown".
      anonymousUsageStats: '=?',
    },
    templateUrl,
    link: function(scope) {
      scope.util = util;

      scope.sendFeedback = function() {
        util.reportError({ title: 'Questions or comments?' });
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
