// One legend panel for graph visualizations.
'use strict';

angular.module('biggraph').directive('graphViewLegend', function() {
  return {
    restrict: 'E',
    scope: {
      data: '=', // List of lines to show.
      side: '@', // left or right
    },
    templateUrl: 'scripts/project/graph-view-legend.html',
    link: function(scope) {
      scope.format = new Intl.NumberFormat('en-US', { maximumFractionDigits: 1 }).format;
    },
  };
});
