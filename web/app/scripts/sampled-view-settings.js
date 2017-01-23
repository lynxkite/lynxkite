'use strict';

angular.module('biggraph').directive('sampledViewSettings', function() {
  return {
    scope: { side: '=' },
    restrict: 'E',
    templateUrl: 'sampled-view-settings.html',
    link: function(scope) {
      scope.x = function() {};
    },
  };
});
