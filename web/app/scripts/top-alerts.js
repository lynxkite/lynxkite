'use strict';

angular.module('biggraph').directive('topAlerts', function() {
  return {
    restrict: 'E',
    templateUrl: 'top-alerts.html',
    link: function(scope) {
      scope.alerts = [];
      scope.$on('topAlert', function(evt, msg) {
        scope.alerts.push({
          message: msg.message,
          details: msg.details,
          time: new Date(),
        });
      });
      scope.close = function(i) {
        scope.alerts.splice(i, 1);
      };
      scope.mailto = function(alert) {
        var support = 'pizza-support@lynxanalytics.com';
        var body = 'Happened at ' + window.location.href + ' on ' + alert.time + '\n\nPlease advise.';
        if (alert.details) {
          body += '\n\nExtra info:\n\n' + JSON.stringify(alert.details, null, '  ');
        }
        return (
          'mailto:' + support +
          '?subject=' + encodeURIComponent('[Issue] ' + alert.message) +
          '&body=' + encodeURIComponent(body)
          );
      };
    }
  };
});
