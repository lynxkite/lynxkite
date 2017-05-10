// Constructs and sends the diagram request.
// The result is then rendered by the "graph-view" directive.
'use strict';

angular.module('biggraph').directive('projectGraph', function (util, loadGraph) {
  return {
    restrict: 'E',
    scope: {
      left: '=',
      right: '=',
      leftToRightBundle: '=',
      rightToLeftBundle: '=',
      contextMenu: '=' },
    replace: false,
    templateUrl: 'scripts/project/project-graph.html',
    link: function(scope) {
      scope.graph = new loadGraph.Graph();
      function updateGraph() {
        scope.graph.load(scope.left, scope.right, scope.leftToRightBundle, scope.rightToLeftBundle);
      }
      util.deepWatch(scope, 'left', updateGraph);
      util.deepWatch(scope, 'right', updateGraph);
      util.deepWatch(scope, 'leftToRightBundle', updateGraph);
      util.deepWatch(scope, 'rightToLeftBundle', updateGraph);

      scope.contextMenu = {
        enabled: false,
        x: 0,
        y: 0,
        data: {}
      };
    },
  };
});
