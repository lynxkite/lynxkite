// A graph visualization that is fully configured using string attributes.
// This means that it can be embedded in help pages.
'use strict';

angular.module('biggraph').directive('embeddedVisualization', function(side, loadGraph) {
  return {
    restrict: 'E',
    scope: {
      width: '=',
      height: '=',
    },
    templateUrl: 'embedded-visualization.html',
    link: function(scope, element, attrs) {
      scope.side = new side.Side([]);
      scope.side.state = angular.extend(side.defaultSideState(), attrs);
      scope.side.reload();
      scope.graph = new loadGraph.Graph();
      scope.$watch('side.project.$resolved', function(loaded) {
        if (loaded) {
          scope.side.onProjectLoaded();
          scope.graph.load(scope.side.viewData);
        }
      });
    },
  };
});
