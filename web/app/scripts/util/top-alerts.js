// A list of error messages that hover over the page.
'use strict';
import '../app';
import './util';
import templateUrl from './top-alerts.html?url';

angular.module('biggraph').directive('topAlerts', function(util) {
  return {
    restrict: 'E',
    templateUrl,
    link: function(scope) {
      scope.alerts = [];
      scope.$on('topAlert', function(evt, msg) {
        scope.alerts.push({
          message: msg.message,
          details: msg.details,
          time: new Date(),
        });
        // Avoid huge stack of errors. Only keep last 3.
        scope.alerts = scope.alerts.slice(-3);
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
