'use strict';

angular.module('biggraph').directive('selectedVisualization', function() {
  return {
    restrict: 'E',
    scope: {
      side: '=',
      visualization: '=',
      title: '=',
    },
    templateUrl: 'selected-visualization.html',
  };
});
