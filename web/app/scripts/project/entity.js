// The entry for an attribute/scalar/segmentation in the project view.
'use strict';

angular.module('biggraph').directive('entity', function($timeout, axisOptions, util) {
  /* globals chroma */
  return {
    restrict: 'E',
    scope: {
      entity: '=',
      kind: '@',
      side: '=',
    },
    templateUrl: 'scripts/project/entity.html',
    link: function(scope, element) {
      /* global Drop */
      // Angular element for easier access of popup elements.
      const dropElement = element.children('.menu');
      let drop = new Drop({
        target: element.children('.token')[0],
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
      // Reset menu state when opening.
      scope.menu = {};
      drop.on('open', function() {
        $timeout(function() {
          scope.menu = { open: true };
          updateHistogram();
        });
      });
      drop.on('close', function() {
        $timeout(function() {
          scope.menu.open = false;
        });
      });
      scope.$on('$destroy', function() {
        if (drop) {
          drop.destroy();
          drop = undefined;
        }
      });

      scope.closeMenu = function() { // For testing.
        scope.$apply(function() {
          drop.close();
        });
      };

      // Attributes and scalars have a "title", segmentations have a "name".
      scope.title = function() { return scope.entity.title || scope.entity.name; };
      scope.isVertexAttribute = function() { return scope.kind === 'vertex-attribute'; };
      scope.isEdgeAttribute = function() { return scope.kind === 'edge-attribute'; };
      scope.isScalar = function() { return scope.kind === 'scalar'; };
      scope.isSegmentation = function() { return scope.kind === 'segmentation'; };
      scope.isAttribute = function() {
        return scope.isVertexAttribute() || scope.isEdgeAttribute();
      };
      scope.isModel = function() { return scope.entity.typeName === 'Model'; };

      scope.active = function() {
        /* if (scope.isSegmentation() &&
            scope.side.sides[1].state.projectName === scope.entity.fullName) {
          return true;
        } */
        return false;
      };

      scope.getFilter = function() {
        const filters = scope.side.state.filters;
        const title = scope.entity.title;
        if (scope.isVertexAttribute()) { return filters.vertex[title]; }
        if (scope.isEdgeAttribute()) { return filters.edge[title]; }
        if (scope.isSegmentation()) {
          return filters.vertex[scope.entity.equivalentAttribute.title];
        }
        return undefined;
      };

      scope.attributeKind = function() {
        return scope.kind.replace('-attribute', '');
      };

      if (scope.isAttribute()) {
        axisOptions.bind(
          scope, scope.side, scope.attributeKind(), scope.entity.title, 'axisOptions');
      }
      scope.showLogCheckbox = function() {
        if (!scope.entity.isNumeric) { return false; }
        if (scope.histogram) { return true; }
        if (scope.side.state.graphMode === 'bucketed') {
          if (scope.side.state.attributeTitles.x === scope.entity.title) { return true; }
          if (scope.side.state.attributeTitles.y === scope.entity.title) { return true; }
        }
        return false;
      };

      let forceHistogram = false;
      scope.showHistogram = function() {
        forceHistogram = true;
        updateHistogram();
      };
      function updateHistogram() {
        if (!scope.menu.open) { return; }
        if (!scope.histogram && !scope.entity.computeProgress && !forceHistogram) { return; }
        if (!scope.entity.canBucket) { return; }
        const q = {
          attributeId: scope.entity.id,
          vertexFilters: scope.side.nonEmptyVertexFilters(),
          edgeFilters: scope.side.nonEmptyEdgeFilters(),
          numBuckets: 20,
          axisOptions: scope.side.axisOptions(scope.attributeKind(), scope.entity.title),
          edgeBundleId: scope.kind === 'edge-attribute' ? scope.side.project.edgeBundle : '',
          sampleSize: scope.precise ? -1 : 50000,
        };
        scope.histogram = util.get('/ajax/histo', q);
        $timeout(function() {
          // The popup may need to move.
          if (drop) {
            drop.position();
          }
        }, 0, false);
      }

      function updateHistogramTSV() {
        const histogram = scope.histogram;
        if (!histogram || !histogram.$resolved) {
          scope.tsv = '';
          return;
        }
        let tsv = '';
        // Header.
        if (histogram.labelType === 'between') {
          tsv += 'From\tTo\tCount\n';
        } else {
          tsv += 'Value\tCount\n';
        }
        // Data.
        for (let i = 0; i < histogram.sizes.length; ++i) {
          if (histogram.labelType === 'between') {
            tsv += histogram.labels[i] + '\t' + histogram.labels[i + 1];
          } else {
            tsv += histogram.labels[i];
          }
          tsv += '\t' + histogram.sizes[i] + '\n';
        }
        scope.histogramTSV = tsv;
      }
      util.deepWatch(scope, 'side.state', updateHistogram);
      scope.$watch('precise', updateHistogram);
      scope.$watch('histogram.$resolved', updateHistogramTSV);

      scope.availableVisualizations = function() {
        if (scope.kind === 'vertex-attribute') { return vertexAttributeVisualizations(); }
        if (scope.kind === 'edge-attribute') { return edgeAttributeVisualizations(); }
        return [];
      };
      scope.availableVisualizationsLowerCase = function() {
        return scope.availableVisualizations().map(function(x) { return x.toLowerCase(); });
      };

      scope.colors = function(cm) {
        if (scope.isFilter('slider')) {
          const cs = util.sliderColorMaps[cm];
          return [cs[0] + ' 45%', 'white 45%, white 55%', cs[1] + ' 55%'];
        } else if (util.qualitativeColorMaps.includes(cm)) {
          const cs = chroma.brewer[cm];
          // Set up gradient to have sharp boundaries.
          return cs.map((c, i) =>
            `${c} ${Math.floor(100 * i / cs.length)}%, ${c} ${Math.floor(100 * (i + 1) / cs.length)}%`
          );
        } else if (cm.includes(' 🗘')) {
          return chroma.brewer[cm.replace(' 🗘', '')].slice().reverse();
        } else {
          return chroma.brewer[cm];
        }
      };
      // We only offer the sequential and divergent color maps for numerical attributes.
      scope.availableColorMaps = function() {
        if (scope.isFilter('slider')) {
          return ['Blue to orange', 'Orange to blue', 'Visible to invisible', 'Invisible to visible'];
        } else if (scope.entity.typeName === 'number') {
          const cms = Object.keys(chroma.brewer).filter(k =>
            k[0] === k[0].toUpperCase() && !util.qualitativeColorMaps.includes(k));
          // Also offer reversed versions.
          cms.push(...cms.map(cm => cm + ' 🗘'));
          cms.sort();
          return cms;
        } else {
          return util.qualitativeColorMaps;
        }
      };
      scope.colorMapKind = function() {
        if (scope.kind === 'vertex-attribute' && scope.isFilter('color')) {
          return 'vertexColorMap';
        }
        if (scope.kind === 'edge-attribute' && scope.isFilter('edge color')) {
          return 'edgeColorMap';
        }
        if (scope.kind === 'vertex-attribute' && scope.isFilter('label color')) {
          return 'labelColorMap';
        }
        if (scope.isFilter('slider')) {
          return 'sliderColorMap';
        }
      };
      scope.isSelectedColorMap = function(cm) {
        let state = scope.side.state[scope.colorMapKind()];
        // We have two defaults. If the state is undefined, this is a visualization from
        // before color maps were added. We use the old colors. (LynxKite Classic / Rainbow)
        // Otherwise we use the preferred color map (Viridis / LynxKite Colors) as the default
        // if the current selection is for the wrong type.
        if (scope.isFilter('slider')) {
          if (!scope.availableColorMaps().includes(state)) {
            state = 'Blue to orange';
          }
        } else if (scope.entity.typeName === 'number') {
          if (state === undefined) {
            state = 'LynxKite Classic';
          } else if (util.qualitativeColorMaps.includes(state)) {
            state = 'Viridis';
          }
        } else {
          if (state === undefined) {
            state = 'Rainbow';
          } else if (!util.qualitativeColorMaps.includes(state)) {
            state = 'LynxKite Colors';
          }
        }
        return state === cm;
      };

      scope.isFilter = function (kind) {
        return scope.side.filterApplied([kind], scope.title()).length > 0;
      };

      function vertexAttributeVisualizations() {
        const e = scope.entity;
        const state = scope.side.state;
        const vs = [];
        if (state.graphMode === 'bucketed') {
          if (e.canBucket) {
            vs.push('X');
            vs.push('Y');
          }
        } else if (state.graphMode === 'sampled' && state.display === 'svg') {
          vs.push('Label');
          const hasLabel = state.attributeTitles.label !== undefined;
          if (e.typeName === 'number') {
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
          } else if (e.typeName === 'Vector[number]') {
            vs.push('Geo coordinates');
            vs.push('Position');
          }
          if (e.isNumeric) {
            vs.push('Slider');
          }
        }
        return vs;
      }

      function edgeAttributeVisualizations() {
        const e = scope.entity;
        const state = scope.side.state;
        const vs = [];
        if (state.graphMode === 'bucketed') {
          if (e.typeName === 'number') {
            vs.push('Width');
          }
        } else if (state.graphMode === 'sampled' && state.display === 'svg') {
          vs.push('Edge label');
          if (e.typeName === 'number') {
            vs.push('Edge color');
            vs.push('Width');
          } else if (e.typeName === 'String') {
            vs.push('Edge color');
          }
        }
        return vs;
      }

      scope.discard = function() {
        scope.side.discard(scope.kind, scope.title());
        drop.close();
      };

      scope.duplicate = function() {
        scope.side.duplicate(scope.kind, scope.title());
        drop.close();
      };

      scope.startRenaming = function() {
        scope.menu.renaming = true;
        scope.menu.renameTo = scope.title();
        // Focus #rename-to once it has appeared.
        $timeout(function() { dropElement.find('#rename-to').focus(); });
      };
      scope.applyRenaming = function() {
        if (scope.menu.renameTo !== scope.title()) {
          scope.side.rename(scope.kind, scope.title(), scope.menu.renameTo);
          scope.menu.renaming = false;
          drop.close();
        }
      };

      scope.toggleShowEmoji = function() {
        scope.showEmoji = !scope.showEmoji;
        if (scope.showEmoji) {
          scope.emojiList = util.get('/images/emoji/list.json');
        }
      };

      scope.setIcon = function(icon) {
        scope.side.setIcon(scope.kind, scope.title(), icon).then(function() {
          drop.close();
        });
      };

      scope.clearIcon = function() {
        scope.side.setIcon(scope.kind, scope.title(), undefined).then(function() {
          drop.close();
        });
      };
    },
  };
});
