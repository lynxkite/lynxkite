// A list of error messages that hover over the page.
'use strict';

angular.module('biggraph').directive('topAlerts', function(util) {
  return {
    restrict: 'E',
    templateUrl: 'scripts/util/top-alerts.html',
    link: function(scope) {
      scope.alerts = [];
      scope.$on('topAlert', function(evt, msg) {
        scope.alerts.push({
          message: msg.message,
          details: msg.details,
          time: new Date(),
        });
      });
      scope.$on('clear topAlerts', function() {
        scope.alerts = [];
      });
      scope.close = function(i) {
        scope.alerts.splice(i, 1);
      };
      scope.reportError = util.reportError;
    }
  };
});
