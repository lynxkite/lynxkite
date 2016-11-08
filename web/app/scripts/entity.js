// The entry for an attribute/scalar/segmentation in the project view.
'use strict';

angular.module('biggraph').directive('entity', function(axisOptions) {
  return {
    restrict: 'E',
    scope: {
      entity: '=',
      kind: '=',
      side: '=',
    },
    templateUrl: 'entity.html',
    link: function(scope, element) {
      /* global Drop */
      new Drop({
        target: element.children('.entity')[0],
        content: element.children('.menu')[0],
        openOn: 'click',
        classes: 'drop-theme-menu',
        remove: true, // Remove from DOM when closing.
        position: 'bottom center',
        tetherOptions: {
          // Keep within the page.
          constraints: [{
            to: 'window',
            attachment: 'together',
            pin: true,
          }],
        },
      });

      scope.isVertexAttribute = function() { return scope.kind === 'vertex-attribute'; };
      scope.isEdgeAttribute = function() { return scope.kind === 'edge-attribute'; };
      scope.isScalar = function() { return scope.kind === 'scalar'; };
      scope.isSegmentation = function() { return scope.kind === 'segmentation'; };

      scope.getFilter = function() {
        var filters = scope.side.state.filters;
        var title = scope.entity.title;
        if (scope.isVertexAttribute()) { return filters.vertex[title]; }
        if (scope.isEdgeAttribute()) { return filters.edge[title]; }
        if (scope.isSegmentation()) {
          return filters.vertex[scope.entity.equivalentAttribute.title];
        }
        return undefined;
      };

      axisOptions.bind(scope, scope.side, 'vertex', scope.entity.title, 'axisOptions');
      scope.sampledVisualizations = [
        'size', 'label', 'label size',
        'color', 'opacity', 'label color',
        'icon', 'image', 'slider', 'position',
        'geo'];
      scope.showLogCheckbox = function() {
        if (!scope.entity.isNumeric) { return false; }
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
