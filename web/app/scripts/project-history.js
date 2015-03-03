'use strict';

angular.module('biggraph').directive('projectHistory', function(util) {
  return {
    restrict: 'E',
    scope: { show: '=', side: '=' },
    templateUrl: 'project-history.html',
    link: function(scope) {
      scope.$watch('show', function(show) {
        if (show) {
          scope.history = util.nocache('/ajax/getHistory', {
            project: scope.side.state.projectName,
          });
        }
      });

      function update() {
        var history = scope.history;
        if (history && history.$resolved && !history.$error) {
          for (var i = 0; i < history.steps.length; ++i) {
            var step = history.steps[i];
            step.originalRequest = angular.copy(step.request);
          }
        }
      }
      scope.$watch('history', update);
      scope.$watch('history.$resolved', update);

      scope.unsaved = function(step) {
        return !angular.equals(step.request, step.originalRequest);
      };
      scope.discardChanges = function(step) {
        angular.copy(step.originalRequest, step.request);
      };
    },
  };
});
