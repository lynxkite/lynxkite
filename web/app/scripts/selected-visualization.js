'use strict';

angular.module('biggraph').directive('selectedVisualization', function() {
  return {
    restrict: 'E',
    scope: {
      side: '=',
      visualization: '=',
      title: '=',
    },
    replace: true,
    templateUrl: 'selected-visualization.html',
    link: function(scope) {
      console.log(scope);
    },
  };
});
