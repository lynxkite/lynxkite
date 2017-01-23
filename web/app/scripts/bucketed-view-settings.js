'use strict';

angular.module('biggraph').directive('bucketedViewSettings', function() {
  return {
    scope: { side: '=' },
    restrict: 'E',
    templateUrl: 'bucketed-view-settings.html',
    link: function(scope) {
      scope.x = function() {};
    },
  };
});
