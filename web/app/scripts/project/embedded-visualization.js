// A graph visualization that is fully configured using string attributes.
// This means that it can be embedded in help pages.
'use strict';

angular.module('biggraph').directive('embeddedVisualization', function(util, side, loadGraph) {
  return {
    restrict: 'E',
    scope: {
      width: '=',
      height: '=',
    },
    templateUrl: 'scripts/project/embedded-visualization.html',
    link: function(scope, element, attrs) {
      scope.side = new side.Side([]);
      // Copy settings from attributes to the state. This only works for top-level string
      // settings. We'll have to think up something if we want to set deeper or non-string
      // settings.
      scope.side.state = angular.extend(side.defaultSideState(), attrs);
      scope.side.reload();
      scope.graph = new loadGraph.Graph();
      scope.$watch('side.project.$resolved', function(loaded) {
        if (loaded) {
          scope.side.onProjectLoaded();
        }
      });
      util.deepWatch(scope, 'side.viewData', function(vd) {
        scope.graph.load(vd);
      });
    },
  };
});
