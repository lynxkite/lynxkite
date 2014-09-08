'use strict';

angular.module('biggraph').directive('topAlerts', function() {
  return {
    restrict: 'E',
    templateUrl: 'top-alerts.html',
    link: function(scope) {
      scope.alerts = [];
      function messageOf(err) {
        return {
          message: err.data.error || err.data || (err.config.url + ' ' + err.statusText),
          time: new Date(),
        };
      }
      scope.$on('ajaxError', function(evt, err) {
        scope.alerts.push(messageOf(err));
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
