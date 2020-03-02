// One legend panel for graph visualizations.
'use strict';

angular.module('biggraph').directive('graphViewLegend', function () {
  return {
    restrict: 'E',
    scope: {
      data: '=', // List of lines to show.
      side: '@', // left or right
    },
    templateUrl: 'scripts/project/graph-view-legend.html',
  };
});
