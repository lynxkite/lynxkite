'use strict';

angular.module('biggraph').directive('selectedVisualization', function() {
  return {
    restrict: 'E',
    scope: {
      side: '=',
      visualization: '=',
      attr: '=',
    },
    templateUrl: 'selected-visualization.html',
  };
});
