// The sidebar for graph visualization. It holds the brightness/contrast controls, etc.
'use strict';
import '../app';
import '../util/util';
import templateUrl from './graph-view-sidebar.html?url';

angular.module('biggraph').directive('graphViewSidebar', ['util', function (util) {
  return {
    restrict: 'E',
    scope: {
      graph: '=', // The graph to visualize.
      mapFilters: '=', // (Output) Filter settings for the map tiles.
    },
    templateUrl,
    link: function(scope, element) {
      scope.$watch('graph.view', updateTSV);
      scope.$watch('graph.view.$resolved', updateTSV);
      function updateTSV() {
        // Generate the TSV representation.
        scope.tsv = '';
        const gv = scope.graph.view;
        if (!gv || !gv.$resolved || gv.$error) {
          return;
        }
        const sides = [];
        const left = scope.graph.left;
        const right = scope.graph.right;
        if (left && left.graphMode) { sides.push(left); }
        if (right && right.graphMode) { sides.push(right); }
        for (let i = 0; i < sides.length; ++i) {
          scope.tsv += vertexSetToTSV(i, gv.vertexSets[i], sides[i]);
        }
        for (let i = 0; i < gv.edgeBundles.length; ++i) {
          scope.tsv += edgeBundleToTSV(gv.edgeBundles[i], sides);
        }
      }

      function vertexSetToTSV(index, vs, side) {
        let i, j, v;
        const name = graphName(index);
        let tsv = '\n';
        if (vs.mode === 'sampled') {
          tsv += 'Vertices of ' + name + ':\n';
          tsv += 'id';
          const attrs = [];
          // Turn the object into an array;
          angular.forEach(side.vertexAttrs, function(attr) { if (attr) { attrs.push(attr); } });
          for (i = 0; i < attrs.length; ++i) {
            tsv += '\t' + attrs[i].title;
          }
          tsv += '\n';
          for (i = 0; i < vs.vertices.length; ++i) {
            v = vs.vertices[i];
            tsv += v.id;
            for (j = 0; j < attrs.length; ++j) {
              tsv += '\t' + v.attrs[attrs[j].id].string;
            }
            tsv += '\n';
          }
        } else {
          const xAxis = side.xAttribute || {};
          const yAxis = side.yAttribute || {};
          let xDescription = xAxis.title + ' (horizontal';
          if (vs.xLabelType === 'between') { xDescription += ', lower bounds'; }
          xDescription += ')';
          let yDescription = yAxis.title + ' (vertical';
          if (vs.yLabelType === 'between') { yDescription += ', lower bounds'; }
          yDescription += ')';
          tsv += 'Buckets of ' + name;
          if (xAxis.id && yAxis.id) {
            tsv += ' by ' + yDescription + ' and ' + xDescription + ':\n';
          } else if (xAxis.id) {
            tsv += ' by ' + xDescription + ':\n';
          } else if (yAxis.id) {
            tsv += ' by ' + yDescription + ':\n';
          } else {
            tsv += ':\n';
          }
          const byBucket = {};
          for (i = 0; i < vs.vertices.length; ++i) {
            v = vs.vertices[i];
            byBucket[v.x + ', ' + v.y] = v;
          }
          const xl = vs.xLabelType === 'between' ? vs.xLabels.length - 1 : vs.xLabels.length;
          const yl = vs.yLabelType === 'between' ? vs.yLabels.length - 1 : vs.yLabels.length;
          for (j = 0; j < vs.xLabels.length; ++j) {
            // X-axis header.
            tsv += '\t' + vs.xLabels[j];
          }
          tsv += '\n';
          for (j = 0; j < vs.yLabels.length; ++j) {
            tsv += vs.yLabels[j]; // Y-axis header.
            for (i = 0; j < yl && i < xl; ++i) {
              tsv += '\t' + byBucket[i + ', ' + j].size;
            }
            tsv += '\n';
          }
        }
        return tsv;
      }

      function edgeBundleToTSV(eb, sides) {
        let i, j;
        let tsv = '\n';
        tsv += 'Edges from ' + graphName(eb.srcIdx);
        tsv += ' to ' + graphName(eb.dstIdx) + ':\n';
        tsv += 'src\tdst\tsize';
        const attrs = [];
        if (eb.srcIdx === eb.dstIdx && sides[eb.srcIdx]) {
          // Turn the object into an array;
          angular.forEach(
            sides[eb.srcIdx].edgeAttrs, function(attr) { if (attr) { attrs.push(attr); } });
        }
        for (i = 0; i < attrs.length; ++i) {
          tsv += '\t' + attrs[i].title;
        }
        tsv += '\n';
        for (i = 0; i < eb.edges.length; ++i) {
          const e = eb.edges[i];
          tsv += e.a + '\t' + e.b + '\t' + e.size;
          for (j = 0; j < attrs.length; ++j) {
            tsv += '\t' + e.attrs[attrs[j].id + ':' + attrs[j].aggregator].string;
          }
          tsv += '\n';
        }
        return tsv;
      }

      function graphName(index) {
        return ['the left-side graph', 'the right-side graph'][index] || 'graph ' + (index + 1);
      }

      function updateFilters() {
        const svg = element.parent().find('svg.graph-view');
        let filter = '';
        if (scope.filters.inverted) {
          filter += 'invert(100%) hue-rotate(180deg) ';
        }
        // To improve performance and compatibility, filters that do nothing are omitted.
        const no = noFilters();
        if (scope.filters.contrast !== no.contrast) {
          filter += 'contrast(' + scope.filters.contrast + '%) ';
        }
        if (scope.filters.saturation !== no.saturation) {
          filter += 'saturate(' + scope.filters.saturation + '%) ';
        }
        if (scope.filters.brightness !== no.brightness) {
          filter += 'brightness(' + scope.filters.brightness + '%) ';
        }
        svg.css({ filter: filter, '-webkit-filter': filter });
      }
      function saveFilters() {
        window.localStorage.setItem('graph-filters', JSON.stringify(scope.filters));
      }
      function saveMapFilters() {
        window.localStorage.setItem('map-filters', JSON.stringify(scope.mapFilters));
      }
      function noFilters() {
        return { inverted: false, contrast: 100, saturation: 100, brightness: 100 };
      }
      function baseMapFilters() {
        return { gamma: 0, saturation: 0, brightness: 0 };
      }
      scope.resetFilters = function() {
        scope.filters = noFilters();
        scope.mapFilters = baseMapFilters();
      };
      scope.resetFilters();
      const loadedFilters = window.localStorage.getItem('graph-filters');
      if (loadedFilters) {
        angular.extend(scope.filters, JSON.parse(loadedFilters));
      }
      const loadedMapFilters = window.localStorage.getItem('map-filters');
      if (loadedMapFilters) {
        angular.extend(scope.mapFilters, JSON.parse(loadedMapFilters));
      }
      util.deepWatch(scope, 'filters', function() { saveFilters(); updateFilters(); });
      util.deepWatch(scope, 'mapFilters', function() { saveMapFilters(); });

      // Whether there is a side with a map view.
      scope.mapViewEnabled = function() {
        const sides = [scope.graph.left, scope.graph.right];
        for (let i = 0; i < sides.length; ++i) {
          if (sides[i] && sides[i].vertexAttrs && sides[i].vertexAttrs.geo) {
            return true;
          }
        }
        return false;
      };
    },
  };
}]);
