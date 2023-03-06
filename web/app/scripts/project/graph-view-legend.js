// One legend panel for graph visualizations.
'use strict';
import '../app';
import templateUrl from './graph-view-legend.html?url';

angular.module('biggraph').directive('graphViewLegend', function() {
  return {
    restrict: 'E',
    scope: {
      data: '=', // List of lines to show.
      side: '@', // left or right
    },
    templateUrl,
    link: function(scope) {
      scope.format = new Intl.NumberFormat('en-US', { maximumFractionDigits: 1 }).format;
    },
  };
});
