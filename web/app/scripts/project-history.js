'use strict';

angular.module('biggraph').directive('projectHistory', function(util) {
  return {
    restrict: 'E',
    scope: { show: '=', side: '=' },
    templateUrl: 'project-history.html',
    link: function(scope) {
      scope.history = util.get('/ajax/getHistory', {});

      scope.$watch('show', function() {
      });
    },
  };
});
