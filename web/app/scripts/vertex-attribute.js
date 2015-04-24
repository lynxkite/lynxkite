// The entry for a vertex attribute in the project view.
'use strict';

angular.module('biggraph').directive('vertexAttribute', function(axisOptions) {
  return {
    scope: { attr: '=vertexAttribute', side: '=' },
    templateUrl: 'vertex-attribute.html',
    link: function(scope) {
      axisOptions.bind(scope, scope.side, 'vertex', scope.attr.title, 'axisOptions');
      scope.sampledVisualizations = [
        'size', 'label', 'label size',
        'color', 'opacity', 'label color',
        'icon', 'image', 'slider', 'position',
        'geo'];
      scope.showLogCheckbox = function() {
        if (!scope.attr.isNumeric) { return false; }
        if (scope.histogram) { return true; }
        if (scope.side.state.graphMode === 'bucketed') {
          if (scope.side.state.attributeTitles.x === scope.attr.title) { return true; }
          if (scope.side.state.attributeTitles.y === scope.attr.title) { return true; }
        }
        return false;
      };
    },
  };
});
