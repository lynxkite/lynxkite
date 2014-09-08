'use strict';

angular.module('biggraph').directive('topAlerts', function() {
  return {
    restrict: 'E',
    templateUrl: 'top-alerts.html',
    link: function(scope) {
      scope.alerts = [];
      scope.$on('topAlert', function(evt, msg) {
        scope.alerts.push({
          message: msg,
          time: new Date(),
        });
      });
      scope.close = function(i) {
        scope.alerts.splice(i, 1);
      };
      scope.mailto = function(alert) {
        var support = 'rnd-team@lynxanalytics.com';
        return (
          'mailto:' + support +
          '?subject=' + encodeURIComponent('[Issue] ' + alert.message) +
          '&body=' + encodeURIComponent('Happened at ' + window.location.href + ' on ' + alert.time + '\n\nPlease advise.')
          );
      };
    }
  };
});
