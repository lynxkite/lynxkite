// The entry for an attribute/scalar/segmentation in the project view.
'use strict';

angular.module('biggraph').directive('entity', function(axisOptions, util) {
  return {
    restrict: 'E',
    scope: {
      entity: '=',
      kind: '@',
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
      scope.isAttribute = function() {
        return scope.isVertexAttribute() || scope.isEdgeAttribute();
      };

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

      // Returns a human-presentable string of the entity kind.
      scope.humanKind = function() {
        return scope.kind.replace('-', ' ');
      };

      scope.attributeKind = function() {
        return scope.kind.replace('-attribute', '');
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
          if (scope.side.state.attributeTitles.x === scope.entity.title) { return true; }
          if (scope.side.state.attributeTitles.y === scope.entity.title) { return true; }
        }
        return false;
      };

      var forceHistogram = false;
      scope.showHistogram = function() {
        forceHistogram = true;
        updateHistogram();
      };
      function updateHistogram() {
        if (!scope.histogram && !scope.entity.computeProgress && !forceHistogram) { return; }
        if (!scope.entity.canBucket) { return; }
        var q = {
          attributeId: scope.entity.id,
          vertexFilters: scope.side.nonEmptyVertexFilters(),
          edgeFilters: scope.side.nonEmptyEdgeFilters(),
          numBuckets: 20,
          axisOptions: scope.side.axisOptions(scope.attributeKind(), scope.entity.title),
          edgeBundleId: scope.kind === 'edge-attribute' ? scope.side.project.edgeBundle : '',
          sampleSize: scope.precise ? -1 : 50000,
        };
        scope.histogram = util.get('/ajax/histo', q);
      }

      function updateHistogramTSV() {
        var histogram = scope.histogram;
        if (!histogram || !histogram.$resolved) {
          scope.tsv = '';
          return;
        }
        var tsv = '';
        // Header.
        if (histogram.labelType === 'between') {
          tsv += 'From\tTo\tCount\n';
        } else {
          tsv += 'Value\tCount\n';
        }
        // Data.
        for (var i = 0; i < histogram.sizes.length; ++i) {
          if (histogram.labelType === 'between') {
            tsv += histogram.labels[i] + '\t' + histogram.labels[i + 1];
          } else {
            tsv += histogram.labels[i];
          }
          tsv += '\t' + histogram.sizes[i] + '\n';
        }
        scope.tsv = tsv;
      }
      util.deepWatch(scope, 'side.state', updateHistogram);
      scope.$watch('precise', updateHistogram);
      scope.$watch('histogram.$resolved', updateHistogramTSV);

      scope.availableVisualizations = function() {
        var vs = ['Label'];
        var e = scope.entity;
        var hasLabel = scope.side.state.attributeTitles.label !== undefined;
        if (e.typeName === 'Double') {
          vs.push('Size');
          vs.push('Color');
          vs.push('Opacity');
          if (hasLabel) {
            vs.push('Label size');
            vs.push('Label color');
          }
        } else if (e.typeName === 'String') {
          vs.push('Color');
          if (hasLabel) {
            vs.push('Label color');
          }
          vs.push('Icon');
          vs.push('Image');
        } else if (e.typeName === '(Double, Double)') {
          vs.push('Geo coordinates');
          vs.push('Position');
        }
        if (e.isNumeric) {
          vs.push('Slider');
        }
        return vs;
      };
    },
  };
});
