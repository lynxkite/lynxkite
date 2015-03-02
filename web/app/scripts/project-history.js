'use strict';

angular.module('biggraph').directive('projectHistory', function(util) {
  return {
    restrict: 'E',
    scope: { show: '=', side: '=' },
    templateUrl: 'project-history.html',
    link: function(scope) {
      scope.history = util.get('/ajax/getHistory', {});

      function update() {
        var history = scope.history;
        console.log('history', history);
        if (history.$resolved && !history.$error) {
          console.log('history loaded');
          for (var i = 0; i < history.steps.length; ++i) {
            var step = history.steps[i];
            step.originalParameters = angular.copy(step.parameters);
          }
        }
      }
      scope.$watch('history', update);
      scope.$watch('history.$resolved', update);

      scope.unsaved = function(step) {
        return !angular.equals(step.parameters, step.originalParameters);
      };
    },
  };
});
